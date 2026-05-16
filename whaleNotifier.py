#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Whale Notifier - Kraken WebSocket
Detecta trades grandes ("ballenas") y los guarda en SQLite + notifica por Telegram.

Setup en VPS:
    mkdir ~/whaleNotifier && cd ~/whaleNotifier
    python3 -m venv .venv
    source .venv/bin/activate
    pip install websocket-client requests pandas

Variables de entorno necesarias (o editar directamente abajo):
    export BOT_TOKEN="tu_token_de_telegram"
    export BOT_CHAT_ID="tu_chat_id"
    export ENV="production"   # o "dev" para desarrollo
    export DB_PATH="/root/whaleNotifier/signals.db"   # ruta donde guardar la base de datos
"""

import requests
import json
from websocket import create_connection
import pandas as pd
import time
import re
from datetime import datetime
import math
import os
import traceback
import sqlite3

# ─────────────────────────────────────────────
# CONFIGURACIÓN — edita aquí o usa env vars
# ─────────────────────────────────────────────
ENV        = os.getenv("ENV", "production")
DB_PATH    = os.getenv("DB_PATH", "/root/whaleNotifier/signals.db")

if ENV == "dev":
    BOT_TOKEN   = os.getenv("BOT_TOKEN", "PON_AQUI_TU_TOKEN_DEV")
    BOT_CHAT_ID = os.getenv("BOT_CHAT_ID", "PON_AQUI_TU_CHAT_ID_DEV")
    print("🛠  Modo DESARROLLO")
else:
    BOT_TOKEN   = os.getenv("BOT_TOKEN", "PON_AQUI_TU_TOKEN_PRODUCCION")
    BOT_CHAT_ID = os.getenv("BOT_CHAT_ID", "PON_AQUI_TU_CHAT_ID_PRODUCCION")
    print("🚀 Modo PRODUCCIÓN")

# Umbral mínimo en EUR para considerar una "ballena"
WHALE_MIN_EUR   = 2000
WHALE_MIN_DIFF  = 3      # % de diferencia de precio mínimo


# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────
def init_db():
    """Crea la tabla de señales si no existe."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp     TEXT NOT NULL,
            pair          TEXT NOT NULL,
            side          TEXT NOT NULL,       -- 'b' = buy, 's' = sell
            price_from    REAL,
            price_to      REAL,
            price_diff_pct REAL,
            volume_token  REAL,
            volume_eur    REAL,
            order_type    TEXT,
            num_trades    INTEGER
        )
    """)
    conn.commit()
    conn.close()
    print(f"✅ Base de datos lista en: {DB_PATH}")


def save_signal(tradeDF, pair, volInEUR, priceDiff):
    """Guarda una señal en la base de datos."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO signals
            (timestamp, pair, side, price_from, price_to, price_diff_pct,
             volume_token, volume_eur, order_type, num_trades)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            pair,
            tradeDF["side"].iloc[0],
            float(tradeDF["price"].iloc[0]),
            float(tradeDF["price"].iloc[-1]),
            round(priceDiff, 4),
            round(float(sum(pd.to_numeric(tradeDF["volume"]))), 6),
            round(volInEUR, 2),
            tradeDF["orderType"].iloc[0],
            len(tradeDF)
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️  Error guardando señal: {e}")


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def telegram_bot_sendtext(bot_message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    params = {
        'chat_id'    : BOT_CHAT_ID,
        'parse_mode' : "Markdown",
        'text'       : bot_message
    }
    try:
        response = requests.post(url, params=params, timeout=10)
        return response.json()
    except Exception as e:
        print(f"⚠️  Error enviando Telegram: {e}")


# ─────────────────────────────────────────────
# KRAKEN API
# ─────────────────────────────────────────────
def getPairs():
    pairsJson = requests.get("https://api.kraken.com/0/public/AssetPairs", timeout=15)
    if pairsJson.status_code != 200:
        telegram_bot_sendtext(f"{datetime.now()} | Error obteniendo pares. Código: {pairsJson.status_code}")
        return {}
    return pairsJson.json()['result']


def getEurPairs(wsnames):
    return [ls for ls in wsnames if re.findall("/EUR$|^EUR/", ls)]


def cleanPairs(pairsList):
    for ch in ["'", "[", "]", "/", " "]:
        if ch in str(pairsList):
            pairsList = str(pairsList).replace(ch, "")
    return pairsList


def getEurPrice(wsnames):
    wsnamesValues = list(wsnames.values())
    eurList = getEurPairs(wsnamesValues)
    cleanEurList = cleanPairs(eurList)
    url = f'https://api.kraken.com/0/public/Ticker?pair={cleanEurList}'
    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        telegram_bot_sendtext(f"{datetime.now()} | Error obteniendo precios EUR. Código: {response.status_code}")
        return {}
    rawPrices = response.json()["result"]
    priceEUR = {}
    for k, v in rawPrices.items():
        if re.findall("^EUR", k):
            priceEUR[k.replace("EUR", "") + "EUR"] = round(1 / pd.to_numeric(v["c"][0]), 4)
        elif re.findall("^ZEUR", k):
            priceEUR[k.replace("ZEUR", "") + "ZEUR"] = round(1 / pd.to_numeric(v["c"][0]), 4)
        else:
            priceEUR[k] = pd.to_numeric(v["c"][0])
    return priceEUR


def getNamesForWS(pairs):
    return {key: value.get('wsname') for (key, value) in pairs.items() if value.get('wsname') is not None}


def createTradeMsg(action, wsnames):
    messageWS = {
        "event"        : action,
        "pair"         : list(wsnames.values()),
        "subscription" : {"name": "trade"}
    }
    return json.dumps(messageWS)


def volumeInEUR(wsnames, pair, volume, eurPrices):
    token = pair.split("/")[0]
    try:
        if token == "EUR":
            return abs(volume)
        elif token == "ETH2.S":
            tokenBase = "ETH/EUR"
            pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
            return abs(volume * eurPrices[pairKey] * 0.96)
        else:
            tokenBase = token + "/EUR"
            try:
                pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
                return abs(eurPrices[pairKey] * volume)
            except ValueError:
                if token == "USD":
                    return abs(1 / eurPrices["ZUSDZEUR"] * volume)
                else:
                    tokenBase = token + "/USD"
                    pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
                    return abs(eurPrices[pairKey] * volume)
    except Exception:
        return 0


def anotateVolume(x):
    x = pd.to_numeric(x)
    if x <= 0:
        return x
    zeros = int(math.log10(x))
    if zeros >= 9:
        return f"{round(x/1e9, 2)}B"
    elif zeros >= 6:
        return f"{round(x/1e6, 2)}M"
    elif zeros >= 3:
        return f"{round(x/1e3, 2)}K"
    return x


# ─────────────────────────────────────────────
# WEBSOCKET
# ─────────────────────────────────────────────
def connectToWS(msg):
    """Crea dos conexiones al WebSocket de Kraken (redundancia)."""
    ws = [None, None]
    delay = 5
    while True:
        try:
            ws[0] = create_connection("wss://ws.kraken.com")
            ws[1] = create_connection("wss://ws.kraken.com")
            ws[0].send(msg)
            ws[1].send(msg)
            print("✅ Conectado al WebSocket")
            return ws
        except Exception as e:
            print(f"⚠️  Error conectando WS: {e}. Reintentando en {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)  # backoff exponencial, máximo 60s


def receiveSafeWS(ws):
    """Recibe datos usando conexión primaria o de respaldo."""
    try:
        return ws[0].recv()
    except Exception:
        try:
            return ws[1].recv()
        except Exception as e:
            raise ConnectionError(f"Ambas conexiones WS fallaron: {e}")


# ─────────────────────────────────────────────
# MENSAJE TELEGRAM
# ─────────────────────────────────────────────
def createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs):
    pairTB = pair.replace("/", "")
    token  = pair.split("/")[0]
    base   = pair.split("/")[1]

    tokenNormalized = "BTC" if token == "XBT" else token
    baseNormalized  = "BTC" if base  == "XBT" else base

    volumeToken = round(sum(pd.to_numeric(tradeDF["volume"])), 3)
    volumeBase  = round(sum(pd.to_numeric(tradeDF["volume"]) * pd.to_numeric(tradeDF["price"])), 3)

    sign       = '\U0001F34F' if tradeDF["side"].iloc[0] == "b" else '\U0001F34E'
    whaleSize  = max(1, int(math.log10(max(volInEUR, 1000) / 1000) + 1))
    whaleEmojis = "\U0001F433" * whaleSize

    if priceDiff >= 20:
        changeEmoji = '\U0001F92F'
    elif priceDiff >= 10:
        changeEmoji = '\U0001F911'
    elif priceDiff >= 5:
        changeEmoji = '\U0001F632'
    else:
        changeEmoji = ""

    volInEURAnnotated = anotateVolume(volInEUR)
    firstDate = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[0])).strftime('%Y-%m-%d %H:%M:%S')
    lastDate  = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[-1])).strftime('%S')
    url = f'[{pair}](https://www.tradingview.com/chart/?symbol=KRAKEN:{tokenNormalized}{baseNormalized})'

    marginMessage = ""
    try:
        pairKey    = list(wsnames.keys())[list(wsnames.values()).index(pair)]
        leverage   = {k: v.get('leverage_sell') for k, v in pairs.items() if v.get('leverage_sell') is not None}
        pairLevs   = leverage.get(pairKey, [])
        if pairLevs:
            maxLev     = max(pairLevs)
            direction  = "s" if tradeDF["side"].iloc[0] == "b" else "b"
            size       = math.floor(volumeToken * 0.1)
            dist       = math.floor(abs(priceDiff))
            tol        = round(abs(dist) * 0.15, 2)
            marginMessage = f"\n`/add {tokenNormalized} {baseNormalized} {size} {direction} {dist} {tol} {maxLev}:1`"
    except Exception:
        pass

    return (
        f"#{pairTB}\n"
        f"*{sign}{round(priceDiff, 2)}%* {changeEmoji} {whaleEmojis}: "
        f"{anotateVolume(volumeBase)} {base} ({anotateVolume(volumeToken)} {token} | {volInEURAnnotated}€)\n"
        f"{firstDate}::{lastDate}\n"
        f"{tradeDF['price'].iloc[0]} → {tradeDF['price'].iloc[-1]} {url}"
        f"{marginMessage}"
    )


# ─────────────────────────────────────────────
# BUCLE PRINCIPAL  (sin recursión)
# ─────────────────────────────────────────────
def connectTradeWS():
    init_db()

    print("📡 Obteniendo pares de Kraken...")
    pairs   = getPairs()
    wsnames = getNamesForWS(pairs)
    print(f"📊 Siguiendo {len(wsnames)} pares")

    eurPrices      = getEurPrice(wsnames)
    msg            = createTradeMsg("subscribe", wsnames)
    ws             = connectToWS(msg)
    last_pair_check = datetime.now()
    last_price_update = datetime.now()

    telegram_bot_sendtext("🟢 Whale Notifier levantado")

    while True:
        try:
            # ── Actualizar precios EUR cada minuto ──
            now = datetime.now()
            if (now - last_price_update).seconds >= 60:
                eurPrices = getEurPrice(wsnames)
                last_price_update = now
                print("💱 Precios EUR actualizados", end=" ", flush=True)

            # ── Actualizar pares cada 5 minutos ──
            if (now - last_pair_check).seconds >= 300:
                updatedPairs   = getPairs()
                updatedWsnames = getNamesForWS(updatedPairs)
                last_pair_check = now
                if len(updatedWsnames) != len(wsnames):
                    print(f"🔄 Nuevos pares detectados ({len(wsnames)} → {len(updatedWsnames)}). Reconectando...")
                    pairs   = updatedPairs
                    wsnames = updatedWsnames
                    ws[0].close()
                    ws[1].close()
                    msg = createTradeMsg("subscribe", wsnames)
                    ws  = connectToWS(msg)

            # ── Recibir datos ──
            raw    = receiveSafeWS(ws)
            result = list(json.loads(raw))

            # Ignorar mensajes de control (heartbeat, subscriptions)
            if type(result[0]) != int:
                continue
            if len(result) < 2 or len(result[1]) <= 1:
                continue

            tradeDF = pd.DataFrame(result[1], columns=["price", "volume", "time", "side", "orderType", "misc"])
            tradeDF = tradeDF.sort_values(by=["time"])
            prices  = pd.to_numeric(tradeDF["price"])

            priceDiff = abs(float((prices.iloc[0] - prices.iloc[-1]) * 100 / prices.iloc[0]))
            pair      = result[3]
            volume    = sum(pd.to_numeric(tradeDF["volume"]))
            volInEUR  = volumeInEUR(wsnames, pair, volume, eurPrices)

            # ── Filtro: ¿es ballena? ──
            is_whale = (volInEUR == 0) or (priceDiff > WHALE_MIN_DIFF and volInEUR > WHALE_MIN_EUR)

            if is_whale:
                priceDiff = round(priceDiff, 3)
                print(f"\n🐋 {pair} | {priceDiff}% | {round(volInEUR, 0)}€")

                # Guardar en base de datos
                save_signal(tradeDF, pair, volInEUR, priceDiff)

                # Enviar a Telegram
                TGmsg = createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs)
                telegram_bot_sendtext(TGmsg)
            else:
                print(".", end="", flush=True)

        except ConnectionError as e:
            print(f"\n🔴 WebSocket caído: {e}. Reconectando...")
            telegram_bot_sendtext("🔴 CAÍDO — reconectando...")
            time.sleep(5)
            try:
                ws = connectToWS(msg)
                telegram_bot_sendtext("🟢 Reconectado")
            except Exception:
                time.sleep(30)

        except KeyboardInterrupt:
            print("\n⛔ Detenido por el usuario.")
            telegram_bot_sendtext("⛔ Whale Notifier detenido manualmente.")
            ws[0].close()
            ws[1].close()
            break

        except Exception:
            traceback.print_exc()
            time.sleep(1)


# ─────────────────────────────────────────────
# ENTRADA
# ─────────────────────────────────────────────
if __name__ == "__main__":
    connectTradeWS()
