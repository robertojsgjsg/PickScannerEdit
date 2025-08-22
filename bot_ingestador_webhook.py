import os
import re
import json
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import httpx
from telegram import (
    Update,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, filters
)

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # fallback si no hay tzdata

# ---------- Config ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))

SHEETS_WEBAPP_URL = os.getenv("SHEETS_WEBAPP_URL")

TZ_NAME = os.getenv("TZ", "Europe/Madrid")
DEFAULT_STAKE = float(os.getenv("DEFAULT_STAKE", "1"))
DATE_OUT_FMT = "%d/%m/%Y"  # DD/MM/YYYY

DEDUPE_ENABLED = os.getenv("DEDUPE_ENABLED", "true").lower() == "true"
REDIS_REST_URL = os.getenv("REDIS_REST_URL", "")
REDIS_REST_TOKEN = os.getenv("REDIS_REST_TOKEN", "")
MEMORY_TTL_DAYS = int(os.getenv("MEMORY_TTL_DAYS", "30"))
MEMORY_NAMESPACE = os.getenv("MEMORY_NAMESPACE", "ingestador:v1")

APUESTA_DEFAULT_CELL = os.getenv("APUESTA_DEFAULT_CELL", "J1")
APUESTA_ALLOWED_USER_IDS = {
    int(x.strip()) for x in os.getenv("APUESTA_ALLOWED_USER_IDS", "").split(",") if x.strip().isdigit()
}

ALLOWED_SELECTIONS = ["1", "X", "2", "1X", "X2", "12"]
SELEC_KB = ReplyKeyboardMarkup(
    [ALLOWED_SELECTIONS, ["Otro"]],
    resize_keyboard=True, one_time_keyboard=True
)

# ---------- Conversation states ----------
ASK_TEAMS, ASK_SELECTION, ASK_ODDS, ASK_STAKE, CONFIRM = range(5)

@dataclass
class Draft:
    date: str = ""       # DD/MM/YYYY
    teams: str = ""      # "A vs B"
    selection: str = ""  # final confirmada
    odds: float = 0.0    # final
    stake: float = DEFAULT_STAKE
    result: str = "Pendiente"
    betId: str = ""      # para col. I
    raw: str = ""

    def dedupe_key(self) -> str:
        base = f"{self.date}|{self.teams}|{self.selection}".lower()
        h = hashlib.sha256(base.encode("utf-8")).hexdigest()
        return f"{MEMORY_NAMESPACE}:{h}"

# ---------- Time helpers ----------
def now_tz():
    try:
        if ZoneInfo:
            return datetime.now(ZoneInfo(TZ_NAME))
    except Exception:
        pass
    return datetime.now()

def today_str() -> str:
    return now_tz().strftime(DATE_OUT_FMT)

# ---------- Parsing ----------
def _to_float(num_str: str) -> float:
    return float(num_str.replace(",", ".").strip())

def _parse_date_ddmmyyyy(text: str) -> Optional[str]:
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b", text)
    if not m:
        return None
    d, mth, y = m.groups()
    y = int(y)
    if y < 100:
        y += 2000
    try:
        dt = datetime(year=y, month=int(mth), day=int(d))
        return dt.strftime(DATE_OUT_FMT)
    except ValueError:
        return None

def _parse_teams(text: str) -> Optional[str]:
    pat = re.compile(r"([^\n\-–—]+?)\s*(?:vs\.?|v\.?|[-–—])\s*([^\n]+)", re.IGNORECASE)
    m = pat.search(text)
    if m:
        a = re.sub(r"\s+", " ", m.group(1)).strip()
        b = re.sub(r"\s+", " ", m.group(2)).strip()
        return f"{a} vs {b}"
    return None

def smart_seed(text: str) -> Draft:
    d = Draft(raw=text)
    d.date = _parse_date_ddmmyyyy(text) or today_str()
    d.teams = _parse_teams(text) or ""
    return d  # No autodetectar cuota (siempre preguntar)

# ---------- Upstash (REST) ----------
async def upstash_cmd(command):
    if not (REDIS_REST_URL and REDIS_REST_TOKEN):
        raise RuntimeError("Upstash REST no configurado")
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.post(
            REDIS_REST_URL,
            headers={"Authorization": f"Bearer {REDIS_REST_TOKEN}"},
            json={"command": command},
        )
        r.raise_for_status()
        return r.json()

async def upstash_exists(key: str) -> bool:
    if not (REDIS_REST_URL and REDIS_REST_TOKEN):
        return False
    try:
        data = await upstash_cmd(["EXISTS", key])
        return bool(data.get("result") == 1)
    except Exception:
        return False

async def upstash_setex(key: str, ttl_days: int, value: str) -> None:
    if not (REDIS_REST_URL and REDIS_REST_TOKEN):
        return
    ttl = max(60, ttl_days * 24 * 3600)
    await upstash_cmd(["SETEX", key, str(ttl), value])

async def upstash_set(key: str, value: str) -> None:
    if not (REDIS_REST_URL and REDIS_REST_TOKEN):
        return
    await upstash_cmd(["SET", key, value])

async def upstash_get(key: str) -> Optional[str]:
    if not (REDIS_REST_URL and REDIS_REST_TOKEN):
        return None
    data = await upstash_cmd(["GET", key])
    return data.get("result")

# ---------- Sheets ----------
async def send_to_sheets(draft: Draft) -> dict:
    payload = {
        "date": draft.date,
        "teams": draft.teams,
        "selection": draft.selection,
        "odds": draft.odds,
        "stake": draft.stake,
        "result": draft.result,
        "betId": draft.betId,
    }
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        r = await client.post(SHEETS_WEBAPP_URL, json=payload, follow_redirects=True)
        r.raise_for_status()
        return r.json()

async def read_cell_from_sheets(cell: str) -> dict:
    url = f"{SHEETS_WEBAPP_URL}?action=readCell&cell={cell}"
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        r = await client.get(url, follow_redirects=True)
        r.raise_for_status()
        return r.json()

# ---------- Helpers ----------
def summary_text(d: Draft) -> str:
    return (
        f"📅 Fecha: {d.date}\n"
        f"🏟️ Partidos: {d.teams or '—'}\n"
        f"🎯 Selección: {d.selection}\n"
        f"🧮 Cuota final: {d.odds:.2f}\n"
        f"💶 Stake: {d.stake:g} €\n"
        f"🆔 ID: {d.betId or '—'}\n"
        f"📌 Resultado: {d.result}"
    )

def gen_bet_id() -> str:
    ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    tail = hashlib.sha1(str(ts).encode()).hexdigest()[:4]
    return f"B{ts}{tail}"

def valid_cell_a1(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z]+[0-9]+", s))

def is_allowed_user(user_id: int) -> bool:
    if not APUESTA_ALLOWED_USER_IDS:
        return True
    return user_id in APUESTA_ALLOWED_USER_IDS

# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    msg = (
        "Soy tu bot *ingestador*.\n"
        "Pégame el pronóstico y te pediré datos para guardarlo en Google Sheets.\n\n"
        f"Tu *user_id* es: `{uid}` (por si quieres limitar /apuesta).\n"
        f"Celda por defecto para /apuesta: `{APUESTA_DEFAULT_CELL}`.\n"
        "Cambia con /modificarcelda A1 (p. ej. /modificarcelda J1)."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cmd_apuesta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_allowed_user(uid):
        await update.message.reply_text("No autorizado para /apuesta.")
        return

    cell_key = f"{MEMORY_NAMESPACE}:apuesta_cell:{uid}"
    cell = APUESTA_DEFAULT_CELL
    try:
        v = await upstash_get(cell_key)
        if v:
            cell = v
    except Exception:
        pass

    try:
        data = await read_cell_from_sheets(cell)
        if not data.get("ok"):
            raise RuntimeError(data.get("error", "Error leyendo celda"))
        value = data.get("value", "")
        await update.message.reply_text(f"📈 Apuesta sugerida ({cell}): {value}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error leyendo {cell}: {e}")

async def cmd_modificar_celda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_allowed_user(uid):
        await update.message.reply_text("No autorizado para /modificarcelda.")
        return
    if not context.args:
        await update.message.reply_text("Uso: /modificarcelda J1")
        return
    cell = context.args[0].upper()
    if not valid_cell_a1(cell):
        await update.message.reply_text("Celda inválida. Ej: J1, H2, AA10…")
        return
    key = f"{MEMORY_NAMESPACE}:apuesta_cell:{uid}"
    try:
        await upstash_set(key, cell)
        await update.message.reply_text(f"✅ Celda para /apuesta actualizada a {cell}")
    except Exception as e:
        await update.message.reply_text(f"❌ No pude guardar la celda: {e}")

async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "").strip()
    draft: Optional[Draft] = context.user_data.get("draft")

    # Si ya hay un borrador, enruta según lo que falte
    if isinstance(draft, Draft):
        # 1) Si aún no hay equipos, intenta tomarlos de este mensaje
        if not draft.teams:
            if " vs " in txt or " VS " in txt.upper():
                draft.teams = txt
                context.user_data["draft"] = draft
                await update.message.reply_text(
                    "Selecciona la *apuesta real* que jugaste:",
                    parse_mode="Markdown",
                    reply_markup=SELEC_KB
                )
                return ASK_SELECTION
            else:
                await update.message.reply_text(
                    "No pude reconocer los *equipos*.\nEscríbelos como `Equipo A vs Equipo B`:",
                    parse_mode="Markdown",
                )
                return ASK_TEAMS

        # 2) Si faltaba selección, trata este texto como selección (incluye “Otro” y libre)
        if not draft.selection:
            return await ask_selection(update, context)

        # 3) Si falta la cuota, trata este texto como cuota
        if not draft.odds or draft.odds < 1.01:
            return await ask_odds(update, context)

        # 4) Si ya hay cuota, trata este texto como stake (número o botones)
        return await stake_text(update, context)

    # Si NO hay borrador aún: sembrar desde el mensaje pegado
    draft = smart_seed(txt)
    context.user_data["draft"] = draft

    if not draft.teams:
        await update.message.reply_text(
            "No pude reconocer los *equipos*.\nEscríbelos como `Equipo A vs Equipo B`:",
            parse_mode="Markdown",
        )
        return ASK_TEAMS

    await update.message.reply_text(
        "Selecciona la *apuesta real* que jugaste:",
        parse_mode="Markdown",
        reply_markup=SELEC_KB
    )
    return ASK_SELECTION

async def ask_teams(update: Update, context: ContextTypes.DEFAULT_TYPE):
    draft: Draft = context.user_data.get("draft") or Draft()
    val = update.message.text.strip()
    if " vs " not in val and " VS " not in val.upper():
        await update.message.reply_text("Formato esperado: `Equipo A vs Equipo B`", parse_mode="Markdown")
        return ASK_TEAMS
    draft.teams = val
    context.user_data["draft"] = draft
    await update.message.reply_text(
        "Selecciona la *apuesta real* que jugaste:",
        parse_mode="Markdown",
        reply_markup=SELEC_KB
    )
    return ASK_SELECTION

async def ask_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    draft: Draft = context.user_data.get("draft") or Draft()
    choice = (update.message.text or "").strip().upper()

    if choice == "OTRO":
        await update.message.reply_text(
            "Escribe la selección exacta (texto libre):",
            reply_markup=ReplyKeyboardRemove()
        )
        return ASK_SELECTION

    if choice and (choice in ALLOWED_SELECTIONS):
        draft.selection = choice
    else:
        draft.selection = (update.message.text or "").strip()

    context.user_data["draft"] = draft
    await update.message.reply_text(
        "Escribe la *cuota final* (ej. 1.85 o 1,85):",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )
    return ASK_ODDS

async def ask_odds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    draft: Draft = context.user_data.get("draft") or Draft()
    # leer la cuota escrita (acepta coma)
    try:
        draft.odds = float((update.message.text or "").strip().replace(",", "."))
        if draft.odds < 1.01:
            raise ValueError()
    except Exception:
        await update.message.reply_text("Formato de cuota no válido. Prueba con 1.85 o 1,85.")
        return ASK_ODDS

    context.user_data["draft"] = draft

    # Teclado de respuestas (no inline) para stake
    kb = ReplyKeyboardMarkup(
        [["Usar 1€", "Cambiar importe"]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await update.message.reply_text("¿Stake?", reply_markup=kb)
    return ASK_STAKE

async def ask_stake(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    draft: Draft = context.user_data.get("draft") or Draft()

    if query.data == "stake_default":
        draft.stake = DEFAULT_STAKE
        draft.betId = gen_bet_id()
        context.user_data["draft"] = draft
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirmar", callback_data="confirm"),
             InlineKeyboardButton("✖️ Cancelar", callback_data="cancel")]
        ])
        await query.edit_message_text("Resumen:\n" + summary_text(draft))
        await query.message.reply_text("¿Confirmo envío a Sheets?", reply_markup=kb)
        return CONFIRM

    elif query.data == "stake_change":
        context.user_data["awaiting_stake_input"] = True
        await query.edit_message_text("Escribe el *stake* en € (ej. 1 o 2.5):", parse_mode="Markdown")
        return ASK_STAKE

async def stake_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    draft: Draft = context.user_data.get("draft") or Draft()
    txt = (update.message.text or "").strip().replace(",", ".")
    # Aceptar número de stake aunque no se haya pulsado "Cambiar importe"
    try:
        st = float(txt)
        if st <= 0:
            raise ValueError()
        draft.stake = st
        draft.betId = gen_bet_id()
        context.user_data["draft"] = draft
    except Exception:
        # Si no es número, recordamos cómo continuar sin cerrar la conversación
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Usar 1€", callback_data="stake_default"),
             InlineKeyboardButton("Cambiar importe", callback_data="stake_change")]
        ])
        await update.message.reply_text(
            "Introduce un número para el *stake* (ej. 1 o 2.5), o pulsa un botón:",
            parse_mode="Markdown",
            reply_markup=kb
        )
        return ASK_STAKE

    # Si llegó aquí, tenemos stake válido -> pedir confirmación
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Confirmar", callback_data="confirm"),
         InlineKeyboardButton("✖️ Cancelar", callback_data="cancel")]
    ])
    await update.message.reply_text("Resumen:\n" + summary_text(draft))
    await update.message.reply_text("¿Confirmo envío a Sheets?", reply_markup=kb)
    return CONFIRM

async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    draft: Draft = context.user_data.get("draft") or Draft()

    if query.data == "cancel":
        await query.edit_message_text("Cancelado. No se envió nada.")
        context.user_data.clear()
        return ConversationHandler.END

    if query.data == "confirm":
        # Dedup (opcional)
        if DEDUPE_ENABLED:
            key = draft.dedupe_key()
            try:
                if await upstash_exists(key):
                    await query.edit_message_text("⚠️ Posible duplicado. No envié la fila.")
                    context.user_data.clear()
                    return ConversationHandler.END
            except Exception:
                pass

        # Enviar a Sheets
        try:
            res = await send_to_sheets(draft)
            if not res.get("ok"):
                raise RuntimeError(res.get("error", "Sheets WebApp error"))
            # Marcar dedupe + snapshot
            if DEDUPE_ENABLED:
                try:
                    await upstash_setex(draft.dedupe_key(), MEMORY_TTL_DAYS, json.dumps(asdict(draft)))
                except Exception:
                    pass
            await query.edit_message_text("✅ Enviado a Google Sheets:\n" + summary_text(draft))
        except Exception as e:
            await query.edit_message_text(f"❌ Error enviando a Sheets: {e}")
        finally:
            context.user_data.clear()
        return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Cancelado.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# ---------- App boot ----------
def main():
    if not TOKEN or not SHEETS_WEBAPP_URL or not WEBHOOK_BASE_URL:
        raise SystemExit("Faltan variables: TELEGRAM_BOT_TOKEN, SHEETS_WEBAPP_URL, WEBHOOK_BASE_URL")

    application = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("apuesta", cmd_apuesta),
            CommandHandler("modificarcelda", cmd_modificar_celda),
            MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text),
        ],
        states={
            ASK_TEAMS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_teams),
            ],
            ASK_SELECTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_selection),
            ],
            ASK_ODDS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_odds),
            ],
            ASK_STAKE: [
                CallbackQueryHandler(ask_stake, pattern="^(stake_default|stake_change)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, stake_text),
            ],
            CONFIRM: [
                CallbackQueryHandler(confirm, pattern="^(confirm|cancel)$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    application.add_handler(conv)

    # Webhook
    url_path = TOKEN  # seguridad básica
    webhook_url = f"{WEBHOOK_BASE_URL}/{url_path}"

    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=url_path,
        webhook_url=webhook_url,
    )

if __name__ == "__main__":
    main()









