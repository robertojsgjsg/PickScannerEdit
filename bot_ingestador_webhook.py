#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bot ingestador (Telegram → Google Sheets via Apps Script) — columna a columna

Flujo por pasos (cada dato se envía en su momento):
1) Al recibir los EQUIPOS (texto "A vs B"):
   - alloc (reserva fila) -> set(B, equipos)

2) Al elegir/teclear la SELECCIÓN (1, X, 2, 1X, X2, 12 u "Otro"):
   - set(C, selección)

3) Al escribir la CUOTA (1.85 o 1,85):
   - set(D, cuota)

4) Al responder el STAKE (botón "Usar 1€" o importe):
   - set(E, stake)
   - set(A, fecha hoy DD/MM/YYYY)
   - set(F, "Pendiente")
   - finalize (pone fórmulas G/H en esa fila)

Extras:
- /apuesta  → lee una celda (J1 por defecto) del Web App ?action=readCell&cell=J1
- /B<betId> <G|P|N>  → actualiza resultado por betId (col F) usando action=updateResult
- Dedupe opcional (Upstash REST) en finalize (por fecha|equipos|selección|cuota|stake)

Requisitos entorno (Render → Environment):
- TELEGRAM_BOT_TOKEN
- WEBHOOK_BASE_URL           (ej. https://tu-servicio.onrender.com  SIN barra final)
- SHEETS_WEBAPP_URL          (tu URL /exec del Apps Script)
- SHEETS_READ_CELL           (opcional, por defecto J1)
- REDIS_REST_URL             (opcional, para dedupe)
- REDIS_REST_TOKEN           (opcional, para dedupe)
- MEMORY_TTL_DAYS=30         (opcional)
- MEMORY_NAMESPACE=ingestador:v1 (opcional)
- APUESTA_ALLOWED_USER_ID    (opcional, restringe /apuesta a tu user_id numérico)
- TZ=Europe/Madrid           (opcional, por si lo usas en logs)
"""

import os
import re
import json
import hmac
import hashlib
import datetime
from dataclasses import dataclass, asdict

import httpx
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

# ------------------------- Config -------------------------

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
WEBHOOK_BASE_URL = os.environ["WEBHOOK_BASE_URL"].rstrip("/")
SHEETS_WEBAPP_URL = os.environ["SHEETS_WEBAPP_URL"].strip()

SHEETS_READ_CELL = os.getenv("SHEETS_READ_CELL", "J1")
APUESTA_ALLOWED_USER_ID = os.getenv("APUESTA_ALLOWED_USER_ID")  # numérico como str

REDIS_REST_URL = os.getenv("REDIS_REST_URL", "").strip()
REDIS_REST_TOKEN = os.getenv("REDIS_REST_TOKEN", "").strip()
MEMORY_TTL_DAYS = int(os.getenv("MEMORY_TTL_DAYS", "30"))
MEMORY_NAMESPACE = os.getenv("MEMORY_NAMESPACE", "ingestador:v1")

PORT = int(os.getenv("PORT", "10000"))

# ------------------------- Estados -------------------------

ASK_TEAMS, ASK_SELECTION, ASK_SELECTION_FREE, ASK_ODDS, ASK_STAKE = range(5)

SELECTION_CHOICES = [["1", "X", "2"], ["1X", "X2", "12"], ["Otro"]]
STAKE_CHOICES = [[KeyboardButton("Usar 1€")], [KeyboardButton("Cambiar importe")]]

TEAMS_REGEX = re.compile(r"^(.+?)\s+vs\s+(.+?)$", re.IGNORECASE)

# ------------------------- Modelos -------------------------

@dataclass
class Draft:
    row: int | None = None
    betId: str | None = None
    date: str | None = None       # DD/MM/YYYY
    teams: str | None = None
    selection: str | None = None
    odds: float | None = None
    stake: float | None = None

# ------------------------- Helpers HTTP -------------------------

async def http_post_json(url: str, payload: dict) -> dict:
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        return r.json()

async def http_get_json(url: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()

# ------------------------- Sheets WebApp (alloc/set/finalize) -------------------------

async def sheets_alloc(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reserva fila: escribe betId en I y devuelve row/betId."""
    j = await http_post_json(SHEETS_WEBAPP_URL, {"action": "alloc"})
    context.user_data["row"] = j["row"]
    context.user_data["betId"] = j["betId"]

async def sheets_set(row: int, col: str, value) -> None:
    await http_post_json(SHEETS_WEBAPP_URL, {"action": "set", "row": row, "col": col, "value": value})

async def sheets_finalize(row: int) -> None:
    await http_post_json(SHEETS_WEBAPP_URL, {"action": "finalize", "row": row})

async def sheets_update_result(bet_id: str, result: str) -> dict:
    return await http_post_json(SHEETS_WEBAPP_URL, {"action": "updateResult", "betId": bet_id, "result": result})

async def sheets_read_cell(a1: str) -> str:
    j = await http_get_json(SHEETS_WEBAPP_URL, {"action": "readCell", "cell": a1})
    return str(j.get("value", ""))

def today_ddmmyyyy() -> str:
    return datetime.datetime.now().strftime("%d/%m/%Y")

# ------------------------- Upstash dedupe (opcional) -------------------------

def _dedupe_key(fingerprint: str) -> str:
    return f"{MEMORY_NAMESPACE}:{fingerprint}"

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

async def dedupe_exists(fingerprint: str) -> bool:
    if not REDIS_REST_URL or not REDIS_REST_TOKEN:  # dedupe desactivado si faltan
        return False
    url = f"{REDIS_REST_URL}/get/{_dedupe_key(fingerprint)}"
    headers = {"Authorization": f"Bearer {REDIS_REST_TOKEN}"}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 404:
            return False
        r.raise_for_status()
        data = r.json()
        return data.get("result") is not None

async def dedupe_set(fingerprint: str, ttl_days: int = MEMORY_TTL_DAYS) -> None:
    if not REDIS_REST_URL or not REDIS_REST_TOKEN:
        return
    seconds = ttl_days * 24 * 3600
    url = f"{REDIS_REST_URL}/setex/{_dedupe_key(fingerprint)}/{seconds}/{fingerprint}"
    headers = {"Authorization": f"Bearer {REDIS_REST_TOKEN}"}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()

# ------------------------- Conversación -------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Soy tu bot ingestador.\n"
        "Pégame los equipos como: «Equipo A vs Equipo B».\n"
        "Después te pediré selección, cuota y stake.\n\n"
        "Comandos: /apuesta, /cancel\n"
        "Actualizar resultado rápido:  /B<betId> G|P|N  (ej. /BABC123 P)"
    )
    return ASK_TEAMS

def _is_allowed_for_apuesta(user_id: int) -> bool:
    if not APUESTA_ALLOWED_USER_ID:
        return True
    try:
        return int(APUESTA_ALLOWED_USER_ID) == user_id
    except Exception:
        return True

async def cmd_apuesta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_for_apuesta(update.effective_user.id):
        return await update.message.reply_text("No tienes permiso para /apuesta.")
    try:
        cell = SHEETS_READ_CELL
        val = await sheets_read_cell(cell)
        txt = f"📈 Apuesta sugerida ({cell}): {val}".strip()
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"❌ Error leyendo {SHEETS_READ_CELL}: {e}")

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Cancelado. Cuando quieras, envía «Equipo A vs Equipo B».")
    return ConversationHandler.END

async def handle_teams(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    m = TEAMS_REGEX.match(text)
    if not m:
        await update.message.reply_text("Formato no válido. Escribe: «Equipo A vs Equipo B».")
        return ASK_TEAMS

    teams = f"{m.group(1).strip()} vs {m.group(2).strip()}"
    draft: Draft = context.user_data.get("draft") or Draft()
    draft.teams = teams

    # Reserva fila en cuanto tenemos el primer dato y lo escribimos (col B)
    if "row" not in context.user_data:
        await sheets_alloc(context)
    row = context.user_data["row"]
    context.user_data["draft"] = draft

    try:
        await sheets_set(row, "B", draft.teams)
    except Exception as e:
        await update.message.reply_text(f"⚠️ No pude escribir equipos en la hoja: {e}")

    # Preguntar selección
    kb = ReplyKeyboardMarkup(SELECTION_CHOICES, resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("Selecciona la apuesta real que jugaste:", reply_markup=kb)
    return ASK_SELECTION

async def handle_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    choice = (update.message.text or "").strip().upper()
    if choice == "OTRO":
        await update.message.reply_text("Escribe la selección exacta (texto libre):")
        return ASK_SELECTION_FREE

    if choice not in {"1", "X", "2", "1X", "X2", "12"}:
        kb = ReplyKeyboardMarkup(SELECTION_CHOICES, resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text("Opción no válida. Elige de teclado o «Otro».", reply_markup=kb)
        return ASK_SELECTION

    draft: Draft = context.user_data.get("draft") or Draft()
    draft.selection = choice
    context.user_data["draft"] = draft

    # set C
    try:
        row = context.user_data["row"]
        await sheets_set(row, "C", draft.selection)
    except Exception as e:
        await update.message.reply_text(f"⚠️ No pude escribir la selección: {e}")

    await update.message.reply_text("Escribe la cuota final (ej. 1.85 o 1,85):")
    return ASK_ODDS

async def handle_selection_free(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sel = (update.message.text or "").strip()
    if not sel:
        await update.message.reply_text("Escribe la selección exacta (texto libre):")
        return ASK_SELECTION_FREE

    draft: Draft = context.user_data.get("draft") or Draft()
    draft.selection = sel
    context.user_data["draft"] = draft

    # set C
    try:
        row = context.user_data["row"]
        await sheets_set(row, "C", draft.selection)
    except Exception as e:
        await update.message.reply_text(f"⚠️ No pude escribir la selección: {e}")

    await update.message.reply_text("Escribe la cuota final (ej. 1.85 o 1,85):")
    return ASK_ODDS

async def handle_odds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = (update.message.text or "").strip().replace(",", ".")
    try:
        odds = float(raw)
        if odds < 1.01:
            raise ValueError()
    except Exception:
        await update.message.reply_text("Formato de cuota no válido. Prueba con 1.85 o 1,85.")
        return ASK_ODDS

    draft: Draft = context.user_data.get("draft") or Draft()
    draft.odds = odds
    context.user_data["draft"] = draft

    # set D
    try:
        row = context.user_data["row"]
        await sheets_set(row, "D", draft.odds)
    except Exception as e:
        await update.message.reply_text(f"⚠️ No pude escribir la cuota: {e}")

    kb = ReplyKeyboardMarkup(STAKE_CHOICES, resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text("¿Stake?", reply_markup=kb)
    return ASK_STAKE

async def handle_stake(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text.lower().startswith("usar 1"):
        stake = 1.0
    elif text.lower().startswith("cambiar"):
        await update.message.reply_text("Escribe el importe (ej. 1.00):")
        return ASK_STAKE
    else:
        # Intentar parsear número
        raw = text.replace(",", ".")
        try:
            stake = float(raw)
            if stake <= 0:
                raise ValueError()
        except Exception:
            await update.message.reply_text("Formato no válido. Escribe un número (ej. 1.00).")
            return ASK_STAKE

    draft: Draft = context.user_data.get("draft") or Draft()
    draft.stake = stake
    if not draft.date:
        draft.date = today_ddmmyyyy()
    context.user_data["draft"] = draft

    row = context.user_data["row"]
    betId = context.user_data.get("betId")

    # --- Escribir E, A, F y finalizar (G/H) ---
    errors = []

    try:
        await sheets_set(row, "E", draft.stake)
    except Exception as e:
        errors.append(f"E(stake): {e}")

    try:
        await sheets_set(row, "A", draft.date)
    except Exception as e:
        errors.append(f"A(fecha): {e}")

    try:
        await sheets_set(row, "F", "Pendiente")
    except Exception as e:
        errors.append(f"F(resultado): {e}")

    # DEDUPE (opcional) — lo hacemos al final, con todos los datos ya rellenados
    try:
        fp_raw = f"{draft.date}|{draft.teams}|{draft.selection}|{draft.odds}|{draft.stake}"
        fp = _sha256_hex(fp_raw)
        if await dedupe_exists(fp):
            await update.message.reply_text("⚠️ Duplicado detectado (no detengo la escritura, sólo aviso).")
        else:
            await dedupe_set(fp)
    except Exception:
        pass

    try:
        await sheets_finalize(row)
    except Exception as e:
        errors.append(f"finalize(G/H): {e}")

    # Resumen
    summary = (
        "✅ Enviado a Google Sheets:\n"
        f"📅 Fecha: {draft.date}\n"
        f"🏟️ Partidos: {draft.teams}\n"
        f"🎯 Selección: {draft.selection}\n"
        f"🧮 Cuota final: {draft.odds}\n"
        f"💶 Stake: {draft.stake} €\n"
        f"🆔 ID: {betId}\n"
        f"📌 Resultado: Pendiente"
    )
    if errors:
        summary += "\n\n⚠️ Incidencias:\n- " + "\n- ".join(errors)

    await update.message.reply_text(summary)

    # Limpiar estado
    context.user_data.clear()
    return ConversationHandler.END

# ------------------------- Comando /B<betId> G|P|N -------------------------

RESULT_CMD_RE = re.compile(r"^/B([A-Za-z0-9\-\_]+)\s+([GgPpNn])$")

async def handle_quick_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    m = RESULT_CMD_RE.match(text)
    if not m:
        return  # dejar que otros handlers sigan

    bet_id = m.group(1)
    result = m.group(2).upper()
    try:
        j = await sheets_update_result(bet_id, result)
        if j.get("ok"):
            row = j.get("row")
            await update.message.reply_text(
                f"✅ Actualizado: betId={bet_id} → Resultado={result} (fila {row})"
            )
        else:
            await update.message.reply_text(f"❌ No pude actualizar: {j}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# ------------------------- Fallback texto fuera de conversación -------------------------

async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Si no estamos en conversación, intenta iniciar con equipos
    if update.message and update.message.text and TEAMS_REGEX.match(update.message.text.strip()):
        # Simular entrada al flujo
        return await handle_teams(update, context)
    else:
        await update.message.reply_text("Envía «Equipo A vs Equipo B» para empezar, o /start.")
        return ConversationHandler.END

# ------------------------- Main / Webhook -------------------------

def main():
    application = Application.builder().token(TOKEN).build()

    # Conversación principal
    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            # Si el usuario pega directamente equipos, también inicia:
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_teams),
        ],
        states={
            ASK_TEAMS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_teams)],
            ASK_SELECTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_selection)],
            ASK_SELECTION_FREE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_selection_free)],
            ASK_ODDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_odds)],
            ASK_STAKE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_stake)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
        per_chat=True,
        per_user=True,
    )

    # Handlers adicionales
    application.add_handler(conv)
    application.add_handler(CommandHandler("apuesta", cmd_apuesta))
    application.add_handler(CommandHandler("cancel", cmd_cancel))
    # /B<betId> G|P|N (regex handler como "message" para capturarlo siempre)
    application.add_handler(MessageHandler(filters.Regex(RESULT_CMD_RE), handle_quick_result))
    # Fallback para otros textos fuera de conv:
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text))

    # Webhook PTB 21
    webhook_url = f"{WEBHOOK_BASE_URL}/{TOKEN}"
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=TOKEN,
        webhook_url=webhook_url,
    )

if __name__ == "__main__":
    main()
