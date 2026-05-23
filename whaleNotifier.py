#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Whale notificator in python:

    Created on Sat Jan 30 15:57:19 2021

@author: Juanma
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
import threading
from flask import Flask, jsonify, render_template_string, request as flask_request

#Definir environmets
myhost = os.uname()[1]

if("localhost" in myhost):
    bot_token = os.getenv("BOT_TOKEN", "")
    bot_chatID = os.getenv("BOT_CHAT_ID", "")
    print("Developing")
else:
    bot_token = os.getenv("BOT_TOKEN", "")
    bot_chatID = os.getenv("BOT_CHAT_ID", "")
    print("Production")

ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
# Base de datos
DB_PATH = os.getenv("DB_PATH", "/data/signals.db")

# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")  # Permite lecturas concurrentes
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp      TEXT NOT NULL,
            pair           TEXT NOT NULL,
            side           TEXT NOT NULL,
            price_from     REAL,
            price_to       REAL,
            price_diff_pct REAL,
            volume_token   REAL,
            volume_eur     REAL,
            order_type     TEXT,
            num_trades     INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_tracking (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id   INTEGER NOT NULL,
            pair        TEXT NOT NULL,
            timestamp   TEXT NOT NULL,
            minutes     INTEGER NOT NULL,
            price       REAL NOT NULL,
            pct_change  REAL,
            FOREIGN KEY (signal_id) REFERENCES signals(id)
        )
    """)
    conn.commit()
    conn.close()
    print(f"BD lista en: {DB_PATH}")

db_lock = threading.Lock()

# Contador de señales recientes por par — para la alerta Twilio
from collections import defaultdict
_recent_signals = defaultdict(list)  # pair -> [timestamps]
_recent_lock = threading.Lock()
RAFAGA_WINDOW_MIN = 10080  # ventana de tiempo en minutos (7 dias)
RAFAGA_MIN_COUNT  = 2     # señales minimas para disparar llamada

def save_signal(tradeDF, pair, volInEUR, priceDiff):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_PATH, timeout=15)
            conn.execute("PRAGMA journal_mode=WAL")
            cursor = conn.execute("""
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
            signal_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return signal_id
    except Exception as e:
        print(f"Error guardando en BD: {e}")
        return None

# ─────────────────────────────────────────────
# PRICE TRACKING
# ─────────────────────────────────────────────
def get_current_price(pair):
    try:
        pair_clean = pair.replace("/", "")
        url = f"https://api.kraken.com/0/public/Ticker?pair={pair_clean}"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        result = r.json().get("result", {})
        if not result:
            return None
        ticker = list(result.values())[0]
        return float(ticker["c"][0])
    except Exception as e:
        print(f"Error obteniendo precio de {pair}: {e}")
        return None

def shorten_url(long_url):
    """Acorta una URL usando TinyURL API."""
    try:
        r = requests.get(f'https://tinyurl.com/api-create.php?url={long_url}', timeout=5)
        if r.status_code == 200 and r.text.startswith('http'):
            return r.text.strip()
    except Exception:
        pass
    return long_url


_ticker_cache = {}
_ticker_cache_time = {}
TICKER_CACHE_TTL = 300  # 5 minutos

# Cache CoinGecko 7d
_cg_cache = {}
_cg_cache_time = {}
CG_CACHE_TTL = 3600  # 1 hora

KRAKEN_TO_CG = {
    "XBT":"bitcoin","BTC":"bitcoin","ETH":"ethereum","XRP":"ripple",
    "ADA":"cardano","SOL":"solana","DOT":"polkadot","DOGE":"dogecoin",
    "AVAX":"avalanche-2","MATIC":"matic-network","POL":"matic-network",
    "LINK":"chainlink","UNI":"uniswap","ATOM":"cosmos","LTC":"litecoin",
    "BCH":"bitcoin-cash","ALGO":"algorand","XLM":"stellar","NEAR":"near",
    "FTM":"fantom","SAND":"the-sandbox","MANA":"decentraland",
    "AAVE":"aave","DASH":"dash","EOS":"eos","TRX":"tron",
    "FIL":"filecoin","ETC":"ethereum-classic","ZEC":"zcash","XMR":"monero",
    "HYPE":"hyperliquid","ASRR":"assister",
}

_cg_id_cache = {}

def get_coingecko_id(token):
    t = token.upper()
    if t in _cg_id_cache:
        return _cg_id_cache[t]
    if t in KRAKEN_TO_CG:
        _cg_id_cache[t] = KRAKEN_TO_CG[t]
        return KRAKEN_TO_CG[t]
    # Buscar en CoinGecko por simbolo exacto
    try:
        r = requests.get(f"https://api.coingecko.com/api/v3/search?query={t}", timeout=8)
        if r.status_code == 200:
            coins = r.json().get("coins", [])
            # Primero buscar coincidencia exacta de simbolo
            for coin in coins:
                if coin.get("symbol","").upper() == t:
                    _cg_id_cache[t] = coin["id"]
                    return coin["id"]
            # Si no hay exacta, usar el primero que aparece
            if coins:
                _cg_id_cache[t] = coins[0]["id"]
                return coins[0]["id"]
    except Exception:
        pass
    result = t.lower()
    _cg_id_cache[t] = result
    return result

def get_7d_change(pair):
    import time as _time
    token = pair.split("/")[0] if "/" in pair else pair
    for suffix in [".S",".P",".M","2"]:
        token = token.replace(suffix, "")
    key = "7d_" + token
    now = _time.time()
    if key in _cg_cache and now - _cg_cache_time.get(key, 0) < CG_CACHE_TTL:
        return _cg_cache[key]
    try:
        cg_id = get_coingecko_id(token)
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_7d_change=true"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json().get(cg_id, {})
            c7 = data.get("usd_7d_change")
            if c7 is not None:
                result = round(c7, 2)
                _cg_cache[key] = result
                _cg_cache_time[key] = now
                return result
    except Exception as e:
        print(f"CoinGecko err {pair}: {e}")
    return None

def get_cmc_url(token):
    t = token.upper()
    for suffix in [".S",".P",".M","2"]:
        t = t.replace(suffix, "")
    if t == "XBT":
        t = "BTC"
    return "https://www.coingecko.com/en/coins/" + get_coingecko_id(t)


# ─────────────────────────────────────────────
# AGENTES IA — Análisis Fundamental y Técnico
# ─────────────────────────────────────────────
_ai_score_cache = {}
_ai_score_cache_time = {}
AI_CACHE_TTL = 3600  # 1 hora por par

SYSTEM_FUNDAMENTAL = """Eres un analista experto en análisis fundamental de criptomonedas.
Tu objetivo es determinar si un proyecto crypto tiene valor real o es especulativo, basándote en datos de mercado.

Devuelve SOLO un JSON con este formato exacto:
{"score": 7.5, "resumen": "Texto breve de máximo 15 palabras explicando el score"}

El score va de 1 a 10:
- 1-3: Memecoin/shitcoin, sin utilidad, muy especulativo o ranking fuera top 1000
- 4-5: Proyecto pequeño o sin diferenciación clara
- 6-7: Proyecto con utilidad moderada, ranking razonable
- 8-9: Proyecto sólido, top 200, buena liquidez y tendencia
- 10: Proyecto excepcional

Claves para el score:
- Categoría "meme" o sin categoría → máximo 4
- Ranking top 50 → mínimo 7
- Ratio Vol/MC > 10% → señal de actividad real
- Sin market cap o sin ranking → máximo 4
- Cambio 30d positivo + top 500 → mínimo 6

Responde SOLO con el JSON puro, sin backticks, sin markdown."""

SYSTEM_TECNICO = """Eres un analista experto en análisis técnico de criptomonedas especializado en detección de mechazos institucionales.
Tu objetivo es evaluar señales de movimientos bruscos de ballenas para determinar si son oportunidades de entrada.
Cuando recibas datos de una señal, devuelve SOLO un JSON con este formato exacto:
{"score": 7.5, "resumen": "Texto breve de máximo 15 palabras explicando el score"}

El score va de 1 a 10 donde:
- 1-3: Señal de baja calidad, probable manipulación o sin contexto favorable
- 4-5: Señal moderada, contexto mixto
- 6-7: Mecho interesante con probabilidad de reversión moderada
- 8-9: Mecho fuerte con alta probabilidad de reversión o continuación
- 10: Señal excepcional con todas las confirmaciones

Factores CLAVE en orden de importancia:
1. INTENSIDAD: >10% = muy fuerte, >5% = fuerte, 2-5% = moderado
2. VOLUMEN RELATIVO: vol_señal/vol_24h. >5% = institucional, >1% = relevante
3. REPETICIÓN: 3+ señales en 7 días = patrón establecido (muy positivo)
4. LADO + PRECIO: SELL cuando precio cerca del HIGH 24h = resistencia clara (reversión alcista probable)
         BUY cuando precio cerca del LOW 24h = soporte claro (reversión bajista probable)
5. HORA: 08-12 UTC y 14-20 UTC = horario de mayor liquidez = señal más fiable
6. VELOCIDAD: múltiples señales en <30min = acumulación/distribución activa
7. CONSISTENCIA: mismo lado repetido = convicción direccional

Estrategia de mecho: la ballena empuja el precio extremo, nosotros ponemos orden límite en ese nivel esperando reversión.
Score alto = alta probabilidad de que el precio vuelva al nivel del mecho.

Responde SOLO con el JSON puro, sin backticks, sin texto adicional, sin markdown."""


def get_coingecko_full(pair):
    """Obtiene datos completos de CoinGecko para el agente fundamental."""
    import time as _time
    token = pair.split("/")[0] if "/" in pair else pair
    for s in [".S",".P",".M","2"]: token = token.replace(s,"")
    cache_key = f"cg_full_{token}"
    now = _time.time()
    if cache_key in _cg_cache and now - _cg_cache_time.get(cache_key,0) < 3600:
        return _cg_cache[cache_key]
    try:
        cg_id = get_coingecko_id(token)
        url = (f"https://api.coingecko.com/api/v3/coins/{cg_id}"
               f"?localization=false&tickers=false&market_data=true"
               f"&community_data=false&developer_data=false")
        r = requests.get(url, timeout=8)
        if r.status_code != 200:
            return {}
        d = r.json()
        md = d.get("market_data", {})
        result = {
            "name":           d.get("name", token),
            "categories":     d.get("categories", [])[:3],
            "market_cap_usd": md.get("market_cap",{}).get("usd"),
            "vol_24h_usd":    md.get("total_volume",{}).get("usd"),
            "price_usd":      md.get("current_price",{}).get("usd"),
            "ath_usd":        md.get("ath",{}).get("usd"),
            "ath_change_pct": md.get("ath_change_percentage",{}).get("usd"),
            "rank":           md.get("market_cap_rank"),
            "change_7d":      md.get("price_change_percentage_7d"),
            "change_30d":     md.get("price_change_percentage_30d"),
            "high_24h":       md.get("high_24h",{}).get("usd"),
            "low_24h":        md.get("low_24h",{}).get("usd"),
            "circulating_supply": md.get("circulating_supply"),
            "total_supply":   md.get("total_supply"),
            "genesis_date":   d.get("genesis_date"),
            "sentiment_up":   d.get("sentiment_votes_up_percentage"),
        }
        _cg_cache[cache_key] = result
        _cg_cache_time[cache_key] = now
        return result
    except Exception as e:
        print(f"CoinGecko full error {pair}: {e}")
        return {}


def ai_fundamental_score(pair, ticker, change_7d):
    """Agente de análisis fundamental con Haiku — evalúa el proyecto con datos de CoinGecko."""
    import time as _time, json as _json, re as _re
    cache_key = f"fund_{pair}"
    now = _time.time()
    if cache_key in _ai_score_cache and now - _ai_score_cache_time.get(cache_key, 0) < AI_CACHE_TTL:
        return _ai_score_cache[cache_key]
    if not ANTHROPIC_KEY:
        return None
    try:
        token = pair.split("/")[0] if "/" in pair else pair
        vol_24h = ticker.get("vol_24h_base", 0) if ticker else 0
        vol_7d  = ticker.get("vol_7d_usd", 0)  if ticker else 0
        chg_24h = ticker.get("change_24h", 0)  if ticker else 0

        cg = get_coingecko_full(pair) or {}
        mktcap  = cg.get("market_cap_usd") or 0
        rank    = cg.get("rank") or "desconocido"
        chg_30d = cg.get("change_30d")
        ath_pct = cg.get("ath_change_pct")
        circ    = cg.get("circulating_supply")
        total   = cg.get("total_supply")
        genesis = cg.get("genesis_date") or "desconocida"
        cats    = ", ".join(cg.get("categories", [])) or "sin categoría"
        name    = cg.get("name", token)
        liq_ratio  = round(vol_24h / mktcap * 100, 2) if mktcap > 0 else None
        supply_pct = round(circ / total * 100, 1) if circ and total and total > 0 else None

        prompt = f"""Evalúa este proyecto crypto solo con los datos disponibles:

TOKEN: {token} | Nombre: {name}
Categoría CoinGecko: {cats}
Ranking global: #{rank}
Market Cap: {f"{mktcap:,.0f} USD" if mktcap else "desconocido"}
Vol 24h: {vol_24h:,.0f} USD | Ratio Vol/MC: {f"{liq_ratio}%" if liq_ratio else "desconocido"}
Vol 7d: {vol_7d:,.0f} USD
Cambio 24h: {chg_24h}% | 7d: {change_7d or "desconocido"}% | 30d: {chg_30d or "desconocido"}%
Distancia ATH: {f"{ath_pct:.1f}%" if ath_pct else "desconocido"}
Supply circulante/total: {f"{supply_pct}%" if supply_pct else "desconocido"}
Antigüedad: {genesis}

Basándote en estos datos, determina si es un proyecto serio o especulativo.
Categorías como "meme","gaming","defi","layer-1","layer-2" ayudan mucho.
Ranking top 200 = proyecto consolidado. Sin ranking = muy especulativo.
Ratio Vol/MC alto = mucha actividad relativa (bueno o manipulación).
Devuelve el JSON con score y resumen."""

        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 150,
                "system": SYSTEM_FUNDAMENTAL,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if r.status_code == 200:
            text = r.json()["content"][0]["text"].strip()
            text = text.replace("```json","").replace("```","").strip()
            match = _re.search(r'\{[^}]+\}', text)
            if match:
                text = match.group(0)
            data = _json.loads(text)
            result = {"score": float(data["score"]), "resumen": data.get("resumen", "")}
            _ai_score_cache[cache_key] = result
            _ai_score_cache_time[cache_key] = now
            return result
        else:
            print(f"AI Fund HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"AI Fundamental error {pair}: {e}")
    return None


def ai_tecnico_score(pair, priceDiff, volInEUR, side, num_signals_7d):
    """Agente de análisis técnico — evalúa la señal."""
    import time as _time, json as _json
    cache_key = f"tec_{pair}_{round(priceDiff,1)}_{side}"
    now = _time.time()
    # Cache corto para técnico (15 min) ya que depende de la señal actual
    if cache_key in _ai_score_cache and now - _ai_score_cache_time.get(cache_key, 0) < 900:
        return _ai_score_cache[cache_key]
    if not ANTHROPIC_KEY:
        return None
    try:
        lado_texto = "COMPRA masiva (bullish)" if side == "b" else "VENTA masiva (bearish)"
        from datetime import datetime
        hora_utc = datetime.utcnow().hour
        horario = "Europa/America (alta liquidez)" if 8 <= hora_utc <= 20 else "Asia/nocturno (baja liquidez)"

        # Contexto de precio (ticker ya disponible desde get_ai_scores)
        # Pasamos los datos extra via num_signals_7d (int) — usamos solo lo disponible
        prompt = f"""Analiza esta señal de mecho de ballena:
- Par: {pair}
- Movimiento de precio: {priceDiff:.2f}%
- Lado de la ballena: {lado_texto}
- Volumen de la operación: {volInEUR:,.0f} USD
- Señales del mismo par en últimos 7 días: {num_signals_7d}
- Hora UTC: {hora_utc}h ({horario})
- Interpretación del mecho: {"Ballena compró fuerte → precio subió → posible venta límite en el extremo alto esperando caída" if side == "b" else "Ballena vendió fuerte → precio cayó → posible compra límite en el extremo bajo esperando rebote"}

Evalúa la probabilidad de reversión del mecho y si conviene poner una orden límite en el nivel extremo.
Devuelve el JSON con score y resumen."""

        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 100,
                "system": SYSTEM_TECNICO,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=15
        )
        if r.status_code == 200:
            text = r.json()["content"][0]["text"].strip()
            # Limpiar backticks de markdown si los hay
            text = text.replace("```json","").replace("```","").strip()
            data = _json.loads(text)
            result = {"score": float(data["score"]), "resumen": data.get("resumen", "")}
            _ai_score_cache[cache_key] = result
            _ai_score_cache_time[cache_key] = now
            return result
    except Exception as e:
        print(f"AI Tecnico error {pair}: {e}")
    return None


def get_ai_scores(pair, priceDiff, volInEUR, side, ticker, change_7d):
    """Obtiene ambos scores y calcula la media. Corre en threads paralelos."""
    import concurrent.futures
    # Contar señales del par en últimos 7 días
    try:
        from datetime import datetime, timedelta
        since_7d = (datetime.utcnow() - timedelta(days=7)).isoformat()
        conn = sqlite3.connect(DB_PATH, timeout=5)
        count_7d = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE pair=? AND timestamp>=?",
            (pair, since_7d)
        ).fetchone()[0]
        conn.close()
    except:
        count_7d = 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f_fund = executor.submit(ai_fundamental_score, pair, ticker, change_7d)
        f_tec  = executor.submit(ai_tecnico_score, pair, priceDiff, volInEUR, side, count_7d)
        # CoinGecko full data en paralelo para pre-cachear
        fund = f_fund.result(timeout=20)
        tec  = f_tec.result(timeout=20)

    if fund and tec:
        score_final = round((fund["score"] + tec["score"]) / 2, 1)
        return {
            "score_final": score_final,
            "score_fund":  fund["score"],
            "score_tec":   tec["score"],
            "resumen_fund": fund["resumen"],
            "resumen_tec":  tec["resumen"]
        }
    elif fund:
        return {"score_final": fund["score"], "score_fund": fund["score"], "score_tec": None,
                "resumen_fund": fund["resumen"], "resumen_tec": ""}
    elif tec:
        return {"score_final": tec["score"], "score_fund": None, "score_tec": tec["score"],
                "resumen_fund": "", "resumen_tec": tec["resumen"]}
    return None


def get_ticker_24h(pair):
    """Obtiene volumen 24h y cambio 24h. Usa CoinGecko para volumen USD preciso."""
    import time as _time
    now = _time.time()
    if pair in _ticker_cache and now - _ticker_cache_time.get(pair, 0) < TICKER_CACHE_TTL:
        return _ticker_cache[pair]
    try:
        # Datos de precio y cambio desde Kraken
        pair_clean = pair.replace('/', '')
        url = f'https://api.kraken.com/0/public/Ticker?pair={pair_clean}'
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        kr = r.json().get('result', {})
        if not kr:
            return None
        t = list(kr.values())[0]

        price_now  = float(t['c'][0])
        vwap_24h   = float(t['p'][1])
        change_24h = round((price_now - vwap_24h) / vwap_24h * 100, 2)
        high_24h   = float(t['h'][1])
        low_24h    = float(t['l'][1])
        vol_24h_token = float(t['v'][1])

        # Volumen en USD: tokens × VWAP 24h (mejor aproximación que precio actual)
        base = pair.split('/')[-1] if '/' in pair else 'USD'
        vol_base = vol_24h_token * vwap_24h  # en moneda base del par
        if base in ('USD', 'USDT'):
            vol_24h_usd = vol_base
        elif base == 'EUR':
            try:
                fx = requests.get('https://api.kraken.com/0/public/Ticker?pair=EURUSD', timeout=5).json()
                eur_usd = float(list(fx['result'].values())[0]['c'][0])
            except:
                eur_usd = 1.08
            vol_24h_usd = vol_base * eur_usd
        elif base == 'GBP':
            try:
                fx = requests.get('https://api.kraken.com/0/public/Ticker?pair=GBPUSD', timeout=5).json()
                gbp_usd = float(list(fx['result'].values())[0]['c'][0])
            except:
                gbp_usd = 1.27
            vol_24h_usd = vol_base * gbp_usd
        else:
            vol_24h_usd = vol_base

        # Intentar mejorar el volumen USD con CoinGecko (más preciso)
        token = pair.split('/')[0] if '/' in pair else pair
        for s in ['.S','.P','.M','2']: token = token.replace(s,'')
        vol_7d_usd = None
        try:
            cg_id = get_coingecko_id(token)
            cg = requests.get(
                f'https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd&include_24hr_vol=true&include_7d_vol=true',
                timeout=5
            ).json()
            cg_data = cg.get(cg_id, {})
            cg_vol = cg_data.get('usd_24h_vol')
            if cg_vol and cg_vol > 0:
                vol_24h_usd = cg_vol
            # usd_7d_vol no existe en CoinGecko - obtenerlo via market_chart
            try:
                cg_id_local = get_coingecko_id(token)
                chart_url = f"https://api.coingecko.com/api/v3/coins/{cg_id_local}/market_chart?vs_currency=usd&days=7"
                chart_r = requests.get(chart_url, timeout=8)
                if chart_r.status_code == 200:
                    vols_7d = chart_r.json().get("total_volumes", [])
                    if vols_7d:
                        vol_7d_usd = round(sum(v[1] for v in vols_7d), 0)
            except Exception:
                pass
        except Exception:
            pass

        result = {
            'vol_24h_token': vol_24h_token,
            'vol_24h_base': round(vol_24h_usd, 0),
            'vol_7d_usd': vol_7d_usd,
            'change_24h': change_24h,
            'high_24h': high_24h,
            'low_24h': low_24h
        }
        _ticker_cache[pair] = result
        _ticker_cache_time[pair] = _time.time()
        return result
    except Exception as e:
        print(f'Error ticker {pair}: {e}')
        return None


def track_price(signal_id, pair, entry_price):
    INTERVAL_MIN = 5
    TOTAL_MIN    = 24 * 60
    steps        = TOTAL_MIN // INTERVAL_MIN
    print(f"📈 Tracking iniciado: {pair} (señal #{signal_id})")
    for step in range(1, steps + 1):
        time.sleep(INTERVAL_MIN * 60)
        minutes = step * INTERVAL_MIN
        price = get_current_price(pair)
        if price is None:
            continue
        pct_change = round((price - entry_price) / entry_price * 100, 4)
        try:
            with db_lock:
                conn = sqlite3.connect(DB_PATH, timeout=15)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("""
                    INSERT INTO price_tracking
                    (signal_id, pair, timestamp, minutes, price, pct_change)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (signal_id, pair, datetime.now().isoformat(), minutes, price, pct_change))
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"Error guardando tracking: {e}")
        if minutes % 60 == 0:
            print(f"📈 {pair} #{signal_id} t+{minutes}min: {price} ({pct_change:+.2f}%)")
    print(f"✅ Tracking completado: {pair} (señal #{signal_id})")

def launch_tracker(signal_id, pair, entry_price):
    if signal_id is None:
        return
    t = threading.Thread(target=track_price, args=(signal_id, pair, entry_price), daemon=True)
    t.start()

# ─────────────────────────────────────────────
# DASHBOARD FLASK
# ─────────────────────────────────────────────
app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🐋 Whale Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;700;800&display=swap" rel="stylesheet">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
:root{--bg:#080c14;--surface:#0d1420;--border:#1a2535;--accent:#00d4ff;--green:#00ff88;--red:#ff4466;--orange:#ffaa00;--purple:#aa88ff;--text:#c8d8e8;--muted:#4a6080}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Mono',monospace;min-height:100vh}
header{padding:1rem 1.5rem;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.5rem}
header h1{font-family:'Syne',sans-serif;font-size:1.4rem;font-weight:800;color:var(--accent)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:0.6rem;padding:1rem 1.5rem;border-bottom:1px solid var(--border)}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.75rem}
.stat-label{font-size:0.58rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em}
.stat-value{font-size:1.3rem;font-weight:700;font-family:'Syne',sans-serif;color:var(--accent);margin-top:0.1rem}
.tabs{display:flex;border-bottom:1px solid var(--border);padding:0 1.5rem;overflow-x:auto}
.tab{padding:0.65rem 1.2rem;cursor:pointer;font-size:0.72rem;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;transition:all 0.2s}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab-content{display:none}
.tab-content.active{display:block}
input,select{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:0.38rem 0.6rem;border-radius:6px;font-family:'Space Mono',monospace;font-size:0.72rem;outline:none}
input:focus,select:focus{border-color:var(--accent)}
button{background:var(--accent);color:#000;border:none;padding:0.38rem 0.85rem;border-radius:6px;font-family:'Syne',sans-serif;font-weight:700;font-size:0.72rem;cursor:pointer;transition:opacity 0.2s}
button:hover{opacity:0.8}
.btn-ghost{background:transparent;border:1px solid var(--border);color:var(--muted);font-size:0.65rem}
.btn-ghost:hover{border-color:var(--accent);color:var(--accent);opacity:1}
.btn-export{background:#1a3050;color:var(--accent);border:1px solid #1a4060}
.btn-export:hover{background:var(--accent);color:#000;opacity:1}
.btn-sm{background:transparent;border:1px solid var(--muted);color:var(--muted);padding:0.1rem 0.4rem;border-radius:4px;font-size:0.6rem;cursor:pointer;font-family:'Space Mono',monospace}
.btn-sm:hover{border-color:var(--accent);color:var(--accent)}
.controls{padding:0.75rem 1.5rem;display:flex;gap:0.5rem;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--border)}
.table-wrap{overflow-x:auto;padding-bottom:2rem}
table{width:100%;border-collapse:collapse;font-size:0.72rem}
th{text-align:left;padding:0.45rem 0.9rem;color:var(--muted);font-size:0.58rem;text-transform:uppercase;letter-spacing:0.07em;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg)}
td{padding:0.4rem 0.9rem;border-bottom:1px solid #0f1a28}
tr:hover td{background:var(--surface)}
.buy{color:var(--green);font-weight:700}
.sell{color:var(--red);font-weight:700}
.pair-tag{background:#0d1e30;border:1px solid #1a3050;border-radius:4px;padding:0.1rem 0.32rem;font-size:0.64rem;color:var(--accent);cursor:pointer}
.pair-tag:hover{border-color:var(--accent)}
.pos{color:var(--green)}
.neg{color:var(--red)}
.neu{color:var(--orange)}
.muted{color:var(--muted)}
.vol{color:#88aacc}
.sec-title{font-family:'Syne',sans-serif;font-size:0.9rem;font-weight:700;color:var(--accent);padding:1rem 1.5rem 0.3rem}
.sec-sub{font-size:0.65rem;color:var(--muted);padding:0 1.5rem 0.75rem;line-height:1.6}
.chip-wrap{display:flex;flex-wrap:wrap;gap:0.3rem;padding:0.6rem 1.5rem;border-bottom:1px solid var(--border)}
.chip{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:0.15rem 0.45rem;font-size:0.65rem;cursor:pointer;transition:all 0.15s}
.chip:hover{border-color:var(--accent);color:var(--accent)}
.chip .cnt{color:var(--muted);margin-left:0.2rem}

/* ANALIZAR CARDS */
.analizar-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:1rem;padding:1.2rem 1.5rem}
.analizar-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden;transition:border-color 0.2s}
.analizar-card:hover{border-color:var(--accent)}
.analizar-card.hot{border-color:var(--red);animation:pulse-red 2s infinite}
.analizar-card.warm{border-color:var(--orange)}
@keyframes pulse-red{0%,100%{box-shadow:0 0 0 0 transparent}50%{box-shadow:0 0 12px 2px rgba(255,68,102,0.3)}}
.card-header{padding:0.85rem 1rem;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:0.5rem;flex-wrap:wrap}
.card-pair{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--accent)}
.card-badges{display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap}
.badge{font-size:0.62rem;font-weight:700;padding:0.18rem 0.5rem;border-radius:20px;font-family:'Syne',sans-serif}
.badge-hot{background:#ff446620;color:var(--red);border:1px solid var(--red)}
.badge-warm{background:#ffaa0020;color:var(--orange);border:1px solid var(--orange)}
.badge-count{background:#00d4ff15;color:var(--accent);border:1px solid var(--accent)}
.badge-side-b{background:#00ff8815;color:var(--green);border:1px solid var(--green)}
.badge-side-s{background:#ff446615;color:var(--red);border:1px solid var(--red)}
.card-metrics{display:grid;grid-template-columns:1fr 1fr 1fr;gap:0;border-bottom:1px solid var(--border)}
.card-metric{padding:0.6rem 0.8rem;border-right:1px solid var(--border)}
.card-metric:last-child{border-right:none}
.card-metric-label{font-size:0.56rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.07em}
.card-metric-value{font-size:0.9rem;font-weight:700;font-family:'Syne',sans-serif;margin-top:0.1rem}
.card-signals{padding:0.6rem 0.8rem;border-bottom:1px solid var(--border)}
.signal-mini{display:flex;gap:0.6rem;align-items:center;font-size:0.66rem;padding:0.2rem 0;border-bottom:1px solid #0a1520}
.signal-mini:last-child{border-bottom:none}
.card-footer{padding:0.6rem 0.8rem;display:flex;gap:0.5rem;align-items:center}
.kraken-btn{display:flex;align-items:center;gap:0.3rem;background:#0a1830;border:1px solid #1a4060;border-radius:6px;padding:0.35rem 0.7rem;font-size:0.65rem;color:var(--accent);text-decoration:none;transition:all 0.2s;font-family:'Space Mono',monospace}
.kraken-btn:hover{background:var(--accent);color:#000;border-color:var(--accent)}

/* MODAL */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.9);z-index:1000;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.4rem;width:93%;max-width:700px;position:relative;max-height:90vh;overflow-y:auto}
.modal h2{font-family:'Syne',sans-serif;font-size:1rem;color:var(--accent);margin-bottom:0.25rem}
.modal-meta{font-size:0.66rem;color:var(--muted);margin-bottom:0.9rem}
.modal-close{position:absolute;top:0.9rem;right:0.9rem;background:transparent;border:1px solid var(--border);color:var(--muted);width:26px;height:26px;border-radius:50%;cursor:pointer;font-size:0.95rem;display:flex;align-items:center;justify-content:center}
.modal-close:hover{border-color:var(--red);color:var(--red);opacity:1}
.chart-container{position:relative;height:240px;margin-bottom:0.9rem}
.no-data{text-align:center;color:var(--muted);padding:2rem;font-size:0.75rem}

/* POR PAR */
.par-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:0.6rem;margin-bottom:1.2rem}
.cand-metric{background:#0a1220;border-radius:6px;padding:0.45rem 0.6rem}
.cand-metric-label{font-size:0.56rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.07em}
.cand-metric-value{font-size:0.85rem;font-weight:700;font-family:'Syne',sans-serif;margin-top:0.1rem}
.rev-grid{display:grid;grid-template-columns:55px 1fr 48px;gap:0.35rem;align-items:center;margin-bottom:0.35rem;font-size:0.66rem}
.rev-bar-wrap{background:#0f1a28;border-radius:4px;height:6px;overflow:hidden}
.rev-bar{height:100%;border-radius:4px}

/* EXPORT */
.export-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:0.9rem;padding:1.2rem 1.5rem}
.export-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.9rem}
.export-card p{font-size:0.65rem;color:var(--muted);margin:0.4rem 0 0.7rem;line-height:1.5}
</style>
</head>
<body>
<header>
  <div><h1>🐋 Whale Dashboard</h1><span id="lastUpdate" style="color:var(--muted);font-size:0.68rem">Cargando...</span></div>
  <button class="btn-ghost" onclick="loadAll()">↻ Actualizar</button>
</header>
<div class="stats" id="stats"></div>
<div class="chip-wrap" id="topPairs"></div>

<div class="tabs">
  <div class="tab active" onclick="switchTab('analizar')">🔍 Analizar</div>
  <div class="tab" onclick="switchTab('par')">📊 Por par</div>
  <div class="tab" onclick="switchTab('signals')">📋 Señales</div>
  <div class="tab" onclick="switchTab('export')">📥 Exportar</div>
</div>

<!-- TAB ANALIZAR -->
<div class="tab-content active" id="tab-analizar">
  <div class="sec-title">🔍 Pares con actividad repetida de ballena</div>
  <div class="sec-sub">Pares con múltiples señales en el periodo seleccionado y al menos 50.000 USD de volumen en 24h. 🔴 = señal en últimos 15min.</div>
  <div class="controls">
    <select id="analizar-window" onchange="loadAnalizar()">
      <option value="30">Últimos 30 min</option>
      <option value="60" selected>Última hora</option>
      <option value="240">Últimas 4h</option>
      <option value="480">Últimas 8h</option>
      <option value="1440">Últimas 24h</option>
    </select>
    <select id="analizar-min" onchange="loadAnalizar()">
      <option value="2" selected>Min 2 señales</option>
      <option value="3">Min 3 señales</option>
      <option value="4">Min 4 señales</option>
    </select>
    <button onclick="loadAnalizar()">Filtrar</button>
    <span id="analizar-count" style="color:var(--muted);font-size:0.7rem"></span>
  </div>
  <div class="analizar-grid" id="analizar-grid">
    <div class="no-data">Cargando...</div>
  </div>
</div>

<!-- TAB POR PAR -->
<div class="tab-content" id="tab-par">
  <div class="sec-title">📊 Análisis por par</div>
  <div class="sec-sub">Historial de señales, % reversión por intervalo y niveles clave.</div>
  <div class="controls">
    <input type="text" id="par-input" placeholder="Ej: BTC/USD" style="width:140px">
    <button onclick="loadPar()">Analizar</button>
  </div>
  <div id="par-content" style="padding:1rem 1.5rem"></div>
</div>

<!-- TAB SEÑALES -->
<div class="tab-content" id="tab-signals">
  <div class="controls">
    <input type="text" id="filterPair" placeholder="Filtrar par">
    <select id="filterSide" onchange="loadSignals()">
      <option value="">Todos</option>
      <option value="b">🍏 Buy</option>
      <option value="s">🍎 Sell</option>
    </select>
    <select id="filterLimit" onchange="loadSignals()">
      <option value="100">100</option>
      <option value="250">250</option>
      <option value="500" selected>500</option>
    </select>
    <button onclick="loadSignals()">Buscar</button>
    <span id="count" style="color:var(--muted);font-size:0.7rem"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>Fecha</th><th>Par</th><th>Lado</th><th>Diff %</th><th>Vol USD</th><th>Cambio 24h</th><th>Vol 24h</th><th>Chart</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- TAB EXPORTAR -->
<div class="tab-content" id="tab-export">
  <div class="sec-title">📥 Exportar para análisis IA</div>
  <div class="sec-sub">Descarga los datos en CSV.</div>
  <div class="export-grid">
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Señales</div><p>Todas las señales con par, lado, precio, volumen.</p><button class="btn-export" onclick="exportCSV('signals')">⬇ Descargar</button></div>
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Mechazos + reversión</div><p>Señales con % reversión a 1h, 4h y 24h.</p><button class="btn-export" onclick="exportCSV('mechazos')">⬇ Descargar</button></div>
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Win Rate</div><p>Ranking de pares por % señales con resultado positivo.</p><button class="btn-export" onclick="exportCSV('winrate')">⬇ Descargar</button></div>
  </div>
</div>

<!-- MODAL -->
<div class="modal-overlay" id="modal">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">×</button>
    <h2 id="modal-title">—</h2>
    <div class="modal-meta" id="modal-meta"></div>
    <div class="chart-container"><canvas id="priceChart"></canvas></div>
    <div id="no-data" class="no-data" style="display:none">⏳ Sin datos de tracking aún</div>
  </div>
</div>

<script>
let chartInstance = null;

function fmt(n,dec=2){
  if(n===null||n===undefined)return '—';
  if(Math.abs(n)>=1e6)return (n/1e6).toFixed(dec)+'M';
  if(Math.abs(n)>=1e3)return (n/1e3).toFixed(dec)+'K';
  return parseFloat(n).toFixed(dec);
}
function pct(v){
  if(v===null||v===undefined)return '<span class="muted">—</span>';
  const c=v>0?'pos':v<0?'neg':'neu';
  return `<span class="${c}">${v>0?'+':''}${parseFloat(v).toFixed(2)}%</span>`;
}
function timeAgo(ts){
  const diff=Math.floor((Date.now()-new Date(ts).getTime())/1000);
  if(diff<60)return diff+'s';
  if(diff<3600)return Math.floor(diff/60)+'min';
  return Math.floor(diff/3600)+'h '+Math.floor((diff%3600)/60)+'min';
}
function krakenUrl(pair){
  const t=pair.split('/')[0]==='XBT'?'BTC':pair.split('/')[0];
  const b=pair.split('/')[1]==='XBT'?'BTC':pair.split('/')[1];
  return `https://pro.kraken.com/app/trade/${t}-${b}`;
}

function switchTab(tab){
  const tabs=['analizar','par','signals','export'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===tab));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  if(tab==='signals') loadSignals();
}

async function loadAll(){
  await Promise.all([loadStats(), loadAnalizar()]);
  document.getElementById('lastUpdate').textContent='Actualizado: '+new Date().toLocaleTimeString();
}

async function loadStats(){
  const s=await(await fetch('/api/stats')).json();
  document.getElementById('stats').innerHTML=`
    <div class="stat"><div class="stat-label">Total señales</div><div class="stat-value">${s.total}</div></div>
    <div class="stat"><div class="stat-label">Compras 🍏</div><div class="stat-value" style="color:var(--green)">${s.buys}</div></div>
    <div class="stat"><div class="stat-label">Ventas 🍎</div><div class="stat-value" style="color:var(--red)">${s.sells}</div></div>
    <div class="stat"><div class="stat-label">Con tracking</div><div class="stat-value" style="color:var(--orange)">${s.tracked}</div></div>
    <div class="stat"><div class="stat-label">Diff media</div><div class="stat-value">${s.avg_diff}%</div></div>
    <div class="stat"><div class="stat-label">Vol medio $</div><div class="stat-value">${fmt(s.avg_vol)}</div></div>`;
  document.getElementById('topPairs').innerHTML=s.top_pairs.map(p=>
    `<div class="chip" onclick="goToPar('${p.pair}')">${p.pair}<span class="cnt">${p.count}</span></div>`).join('');
}

function goToPar(pair){
  switchTab('par');
  document.getElementById('par-input').value=pair;
  loadPar();
}

async function loadAnalizar(){
  const win=document.getElementById('analizar-window').value;
  const min=document.getElementById('analizar-min').value;
  const el=document.getElementById('analizar-grid');
  el.innerHTML='<div class="no-data">Cargando...</div>';

  let groups;
  try {
    groups=await(await fetch(`/api/analizar?window=${win}&min=${min}`)).json();
  } catch(e){
    el.innerHTML='<div class="no-data">Error cargando datos</div>';
    return;
  }

  if(!groups||groups.length===0){
    el.innerHTML='<div class="no-data">No hay pares con actividad repetida en este periodo.<br>Prueba ampliar la ventana de tiempo.</div>';
    document.getElementById('analizar-count').textContent='';
    return;
  }

  document.getElementById('analizar-count').textContent=`${groups.length} pares activos`;

  // Fetch ticker batch for all pairs
  const pairs=groups.map(g=>g.pair).join(',');
  let tickers={};
  try {
    tickers=await(await fetch('/api/ticker_batch?pairs='+encodeURIComponent(pairs))).json();
  } catch(e){}

  // Filter by 50K vol 24h
  const filtered=groups.filter(g=>{
    const t=tickers[g.pair];
    return !t || t.vol_24h_base>=50000;
  });

  if(filtered.length===0){
    el.innerHTML='<div class="no-data">No hay pares con 50.000 USD+ de volumen en 24h en este periodo.</div>';
    return;
  }

  document.getElementById('analizar-count').textContent=`${filtered.length} pares activos (vol 24h >50K$)`;

  el.innerHTML=filtered.map(g=>{
    const t=tickers[g.pair]||{};
    const minAgo=Math.floor((Date.now()-new Date(g.last_signal).getTime())/60000);
    const isHot=minAgo<15;
    const isWarm=minAgo<60;
    const cardClass=isHot?'hot':isWarm?'warm':'';
    const badgeClass=isHot?'badge-hot':'badge-warm';
    const badgeText=isHot?'🔴 AHORA':'🟡 RECIENTE';
    const sideClass=g.dominant_side==='b'?'badge-side-b':'badge-side-s';
    const sideText=g.dominant_side==='b'?'🍏 COMPRA':'🍎 VENTA';
    const changeClass=t.change_24h>0?'pos':t.change_24h<0?'neg':'neu';
    const kUrl=krakenUrl(g.pair);

    const signalRows=(g.signals||[]).slice(0,5).map(s=>`
      <div class="signal-mini">
        <span class="${s.side==='b'?'buy':'sell'}">${s.side==='b'?'🍏':'🍎'}</span>
        <span class="neu">+${s.price_diff_pct}%</span>
        <span class="vol">${fmt(s.volume_eur)}$</span>
        <span class="muted" style="font-size:0.6rem">${s.timestamp.replace('T',' ').substring(11,19)}</span>
        <button class="btn-sm" onclick="openChart(${s.id},'${g.pair}','${s.side}',${s.price_to})" style="margin-left:auto">📈</button>
      </div>`).join('');

    return `<div class="analizar-card ${cardClass}">
      <div class="card-header">
        <div>
          <div class="card-pair">${g.pair}</div>
          <div style="font-size:0.6rem;color:var(--muted);margin-top:0.1rem">hace ${timeAgo(g.last_signal)}</div>
        </div>
        <div class="card-badges">
          <span class="badge ${badgeClass}">${badgeText}</span>
          <span class="badge badge-count">${g.count} señales</span>
          <span class="badge ${sideClass}">${sideText}</span>
          ${g.ai_score!==null&&g.ai_score!==undefined?`<span class="badge" style="background:${g.ai_score>=7?'#00ff8820':g.ai_score>=5?'#ffaa0020':'#ff446620'};color:${g.ai_score>=7?'var(--green)':g.ai_score>=5?'var(--orange)':'var(--red)'};border:1px solid ${g.ai_score>=7?'var(--green)':g.ai_score>=5?'var(--orange)':'var(--red)'}">⭐ ${g.ai_score}/10</span>`:''}
        </div>
      </div>
      <div class="card-metrics">
        <div class="card-metric">
          <div class="card-metric-label">Diff media</div>
          <div class="card-metric-value neu">${g.avg_diff}%</div>
        </div>
        <div class="card-metric">
          <div class="card-metric-label">Cambio 24h</div>
          <div class="card-metric-value ${changeClass}">${t.change_24h!==undefined?(t.change_24h>0?'+':'')+t.change_24h+'%':'—'}</div>
        </div>
        <div class="card-metric">
          <div class="card-metric-label">Cambio 7d</div>
          <div class="card-metric-value ${g.change_7d>0?'pos':g.change_7d<0?'neg':'muted'}">${g.change_7d!==null&&g.change_7d!==undefined?(g.change_7d>0?'+':'')+g.change_7d+'%':'—'}</div>
        </div>
        <div class="card-metric">
          <div class="card-metric-label">Vol 24h</div>
          <div class="card-metric-value vol">${t.vol_24h_base!==undefined?fmt(t.vol_24h_base)+'$':'—'}</div>
        </div>
        <div class="card-metric">
          <div class="card-metric-label">Vol 7d</div>
          <div class="card-metric-value vol">${t.vol_7d_usd!==undefined&&t.vol_7d_usd?fmt(t.vol_7d_usd)+'$':'—'}</div>
        </div>
      </div>
      <div class="card-signals">${signalRows}</div>
      ${g.ai_summary?`<div style="padding:0.4rem 0.8rem;font-size:0.64rem;color:var(--muted);border-top:1px solid var(--border);font-style:italic">💡 ${g.ai_summary}</div>`:''}
      <div class="card-footer">
        <a href="${kUrl}" target="_blank" class="kraken-btn">📊 Kraken</a>
        <a href="${g.cmc_url}" target="_blank" class="kraken-btn" style="background:#0d1f3c;border-color:#1a4080">🦎 CoinGecko</a>
        <button class="btn-sm" onclick="goToPar('${g.pair}')">🔍 Historial</button>
      </div>
    </div>`;
  }).join('');
}

async function loadPar(){
  const pair=document.getElementById('par-input').value.trim();
  if(!pair)return;
  const data=await(await fetch('/api/par?pair='+encodeURIComponent(pair))).json();
  const el=document.getElementById('par-content');
  if(!data||data.total===0){el.innerHTML='<div class="no-data">No hay datos para '+pair+'</div>';return;}
  const r1c=(data.rev_1h_media||0)>50?'pos':'neg';
  const r4c=(data.rev_4h_media||0)>50?'pos':'neg';
  const r24c=(data.rev_24h_media||0)>50?'pos':'neg';
  el.innerHTML=`
    <div class="par-metrics">
      <div class="cand-metric"><div class="cand-metric-label">Total señales</div><div class="cand-metric-value" style="color:var(--accent)">${data.total}</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 1h</div><div class="cand-metric-value ${r1c}">${data.rev_1h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 4h</div><div class="cand-metric-value ${r4c}">${data.rev_4h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 24h</div><div class="cand-metric-value ${r24c}">${data.rev_24h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol mínimo</div><div class="cand-metric-value vol">${fmt(data.vol_min)}$</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol medio</div><div class="cand-metric-value vol">${fmt(data.vol_avg)}$</div></div>
    </div>
    <div style="font-size:0.65rem;color:var(--muted);margin-bottom:0.6rem">REVERSIÓN POR INTERVALO</div>
    <div style="max-width:380px;margin-bottom:1.2rem">
      <div class="rev-grid"><span class="muted">1h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_1h_media||0,100)}%;background:${(data.rev_1h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r1c}">${data.rev_1h_media??'—'}%</span></div>
      <div class="rev-grid"><span class="muted">4h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_4h_media||0,100)}%;background:${(data.rev_4h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r4c}">${data.rev_4h_media??'—'}%</span></div>
      <div class="rev-grid"><span class="muted">24h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_24h_media||0,100)}%;background:${(data.rev_24h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r24c}">${data.rev_24h_media??'—'}%</span></div>
    </div>
    <div style="font-size:0.65rem;color:var(--muted);margin-bottom:0.6rem">SEÑALES HISTÓRICAS</div>
    <div class="table-wrap"><table>
      <thead><tr><th>#</th><th>Fecha</th><th>Lado</th><th>Diff %</th><th>Precio extremo</th><th>Vol $</th><th>Rev 1h</th><th>Rev 4h</th><th>Rev 24h</th><th>Chart</th></tr></thead>
      <tbody>${(data.signals||[]).map(s=>`<tr>
        <td class="muted">#${s.id}</td>
        <td class="muted" style="font-size:0.65rem">${s.timestamp.replace('T',' ').substring(0,16)}</td>
        <td>${s.side==='b'?'<span class="buy">🍏</span>':'<span class="sell">🍎</span>'}</td>
        <td>${pct(s.price_diff_pct)}</td>
        <td class="vol">${parseFloat(s.price_extremo).toPrecision(5)}</td>
        <td class="vol">${fmt(s.volume_eur)}$</td>
        <td>${pct(s.rev_1h)}</td><td>${pct(s.rev_4h)}</td><td>${pct(s.rev_24h)}</td>
        <td><button class="btn-sm" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_extremo})">📈</button></td>
      </tr>`).join('')}</tbody>
    </table></div>`;
}

async function loadSignals(){
  const pair=document.getElementById('filterPair').value;
  const side=document.getElementById('filterSide').value;
  const limit=document.getElementById('filterLimit').value;
  const data=await(await fetch(`/api/signals?pair=${pair}&side=${side}&limit=${limit}`)).json();
  document.getElementById('count').textContent=`${data.length} señales`;
  document.getElementById('tbody').innerHTML=data.map(s=>`<tr>
    <td class="muted">#${s.id}</td>
    <td class="muted" style="font-size:0.65rem">${s.timestamp.replace('T',' ').substring(0,19)}</td>
    <td><span class="pair-tag" onclick="goToPar('${s.pair}')">${s.pair}</span>
      <a href="${krakenUrl(s.pair)}" target="_blank" style="font-size:0.6rem;color:var(--muted);margin-left:0.3rem;text-decoration:none">↗</a>
    </td>
    <td>${s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>'}</td>
    <td>${pct(s.price_diff_pct)}</td>
    <td class="vol">${fmt(s.volume_eur)}$</td>
    <td id="ch24-${s.id}" class="muted" style="font-size:0.7rem">...</td>
    <td id="vol24-${s.id}" class="muted" style="font-size:0.7rem">...</td>
    <td><button class="btn-sm" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_to})">📈</button>
      <a href="${krakenUrl(s.pair)}" target="_blank" class="btn-sm" style="text-decoration:none;display:inline-block;margin-left:2px">K↗</a>
    </td>
  </tr>`).join('')||'<tr><td colspan="9" class="no-data">No hay señales</td></tr>';
  const seen=new Set();
  data.forEach(s=>{if(!seen.has(s.pair)){seen.add(s.pair);loadTicker24h(s.pair,data);}});
}

async function loadTicker24h(pair,signals){
  try {
    const t=await(await fetch('/api/ticker24h?pair='+encodeURIComponent(pair))).json();
    if(!t||t.error)return;
    const cc=t.change_24h>0?'pos':t.change_24h<0?'neg':'neu';
    signals.filter(s=>s.pair===pair).forEach(s=>{
      const chEl=document.getElementById('ch24-'+s.id);
      const volEl=document.getElementById('vol24-'+s.id);
      if(chEl)chEl.innerHTML=`<span class="${cc}">${t.change_24h>0?'+':''}${t.change_24h}%</span>`;
      if(volEl)volEl.innerHTML=`<span class="vol">${fmt(t.vol_24h_base)}$</span>`;
    });
  } catch(e){}
}

async function openChart(signalId,pair,side,entryPrice){
  document.getElementById('modal').classList.add('open');
  document.getElementById('modal-title').textContent=`${pair} — Señal #${signalId}`;
  document.getElementById('modal-meta').textContent='Cargando...';
  document.getElementById('no-data').style.display='none';
  document.getElementById('priceChart').style.display='block';
  if(chartInstance){chartInstance.destroy();chartInstance=null;}
  const data=await(await fetch(`/api/tracking/${signalId}`)).json();
  const pts=data.tracking; const sig=data.signal;
  document.getElementById('modal-meta').textContent=`${sig.side==='b'?'🍏 BUY':'🍎 SELL'} | Entrada: ${parseFloat(sig.price_to).toPrecision(5)} | Vol: ${fmt(sig.volume_eur)}$ | ${sig.timestamp.substring(0,19)}`;
  if(!pts||pts.length===0){document.getElementById('no-data').style.display='block';document.getElementById('priceChart').style.display='none';return;}
  const color=pts[pts.length-1].pct_change>=0?'#00ff88':'#ff4466';
  chartInstance=new Chart(document.getElementById('priceChart'),{type:'line',data:{labels:pts.map(p=>`+${p.minutes}min`),datasets:[{label:'Precio',data:pts.map(p=>p.price),borderColor:color,backgroundColor:color+'15',borderWidth:2,pointRadius:0,tension:0.3,fill:true,yAxisID:'y'},{label:'% cambio',data:pts.map(p=>p.pct_change),borderColor:'#00d4ff',borderWidth:1,borderDash:[4,4],pointRadius:0,tension:0.3,fill:false,yAxisID:'y2'}]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{color:'#4a6080',font:{family:'Space Mono',size:10}}},tooltip:{backgroundColor:'#0d1420',borderColor:'#1a2535',borderWidth:1,titleColor:'#c8d8e8',bodyColor:'#c8d8e8'}},scales:{x:{ticks:{color:'#4a6080',font:{size:9},maxTicksLimit:12},grid:{color:'#0f1a28'}},y:{ticks:{color:'#00ff88',font:{size:9}},grid:{color:'#0f1a28'},position:'left'},y2:{ticks:{color:'#00d4ff',font:{size:9}},grid:{display:false},position:'right'}}}});
}
function closeModal(){document.getElementById('modal').classList.remove('open');if(chartInstance){chartInstance.destroy();chartInstance=null;}}
document.getElementById('modal').addEventListener('click',e=>{if(e.target===document.getElementById('modal'))closeModal();});
function exportCSV(type){window.location.href='/api/export/'+type;}
loadAll();
setInterval(loadAll,60000);
</script>
</body>
</html>
"""

def db_get(query, params=[]):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def db_one(query, params=[]):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    val = conn.execute(query, params).fetchone()[0]
    conn.close()
    return val

def get_reversion(signal_id, minutes_target):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    row = conn.execute("""
        SELECT pct_change FROM price_tracking
        WHERE signal_id=? AND minutes<=?
        ORDER BY minutes DESC LIMIT 1
    """, (signal_id, minutes_target)).fetchone()
    conn.close()
    return round(row[0], 2) if row else None

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/api/signals')
def api_signals():
    pair  = flask_request.args.get('pair', '')
    side  = flask_request.args.get('side', '')
    limit = int(flask_request.args.get('limit', 500))
    q = "SELECT * FROM signals WHERE 1=1"
    p = []
    if pair: q += " AND pair LIKE ?"; p.append(f"%{pair}%")
    if side: q += " AND side = ?"; p.append(side)
    q += " ORDER BY id DESC LIMIT ?"; p.append(limit)
    return jsonify(db_get(q, p))

@app.route('/api/stats')
def api_stats():
    return jsonify({
        'total':    db_one("SELECT COUNT(*) FROM signals"),
        'buys':     db_one("SELECT COUNT(*) FROM signals WHERE side='b'"),
        'sells':    db_one("SELECT COUNT(*) FROM signals WHERE side='s'"),
        'tracked':  db_one("SELECT COUNT(DISTINCT signal_id) FROM price_tracking"),
        'avg_diff': db_one("SELECT ROUND(AVG(price_diff_pct),2) FROM signals"),
        'avg_vol':  db_one("SELECT ROUND(AVG(volume_eur),2) FROM signals"),
        'top_pairs': db_get("SELECT pair, COUNT(*) as count FROM signals GROUP BY pair ORDER BY count DESC LIMIT 15")
    })

@app.route('/api/tracking/<int:signal_id>')
def api_tracking(signal_id):
    rows   = db_get("SELECT minutes,price,pct_change,timestamp FROM price_tracking WHERE signal_id=? ORDER BY minutes", [signal_id])
    signal = db_get("SELECT * FROM signals WHERE id=?", [signal_id])
    return jsonify({"signal": signal[0] if signal else {}, "tracking": rows})

@app.route('/api/analizar')
def api_analizar():
    from datetime import datetime, timedelta
    from collections import defaultdict
    window_min  = int(flask_request.args.get('window', 60))
    min_signals = int(flask_request.args.get('min', 2))
    since = (datetime.utcnow() - timedelta(minutes=window_min)).isoformat()
    signals = db_get("SELECT * FROM signals WHERE timestamp >= ? ORDER BY pair, timestamp ASC", [since])
    if not signals:
        return jsonify([])
    by_pair = defaultdict(list)
    for s in signals:
        by_pair[s['pair']].append(s)
    result = []
    for pair, sigs in by_pair.items():
        if len(sigs) < min_signals:
            continue
        sides = [s['side'] for s in sigs]
        dominant_side = max(set(sides), key=sides.count)
        diffs = [s['price_diff_pct'] for s in sigs]
        vols  = [s['volume_eur'] for s in sigs]
        t_last = sigs[-1]['timestamp']
        token = pair.split('/')[0] if '/' in pair else pair
        # AI score from cache only (don't block the API response)
        ai_cache_key_f = f"fund_{pair}"
        ai_cache_key_t = f"tec_{pair}"
        fund_cached = _ai_score_cache.get(ai_cache_key_f)
        tec_cached  = None
        # Find latest tec score for this pair
        for k, v in _ai_score_cache.items():
            if k.startswith(f"tec_{pair}_"):
                tec_cached = v
                break
        if fund_cached and tec_cached:
            ai_score_final = round((fund_cached["score"] + tec_cached["score"]) / 2, 1)
            ai_summary = tec_cached.get("resumen") or fund_cached.get("resumen") or ""
        elif fund_cached:
            ai_score_final = fund_cached["score"]
            ai_summary = fund_cached.get("resumen", "")
        elif tec_cached:
            ai_score_final = tec_cached["score"]
            ai_summary = tec_cached.get("resumen", "")
        else:
            ai_score_final = None
            ai_summary = ""
        result.append({
            'pair': pair,
            'count': len(sigs),
            'dominant_side': dominant_side,
            'avg_diff': round(sum(diffs)/len(diffs), 2),
            'total_vol': round(sum(vols), 0),
            'last_signal': t_last,
            'signals': sigs,
            'change_7d': get_7d_change(pair),
            'cmc_url': get_cmc_url(token),
            'ai_score': ai_score_final,
            'ai_summary': ai_summary
        })
    result.sort(key=lambda x: x['last_signal'], reverse=True)
    return jsonify(result)

@app.route('/api/par')
def api_par():
    pair = flask_request.args.get('pair', '')
    if not pair: return jsonify({})
    signals = db_get("SELECT * FROM signals WHERE pair LIKE ? ORDER BY id DESC", [f"%{pair}%"])
    if not signals: return jsonify({'total': 0})
    vols=[s['volume_eur'] for s in signals]; diffs=[s['price_diff_pct'] for s in signals]
    revs_1h=[]; revs_4h=[]; revs_24h=[]; enriched=[]
    for s in signals:
        r1=get_reversion(s['id'],60); r4=get_reversion(s['id'],240); r24=get_reversion(s['id'],1440)
        if r1 is not None: revs_1h.append(abs(r1))
        if r4 is not None: revs_4h.append(abs(r4))
        if r24 is not None: revs_24h.append(abs(r24))
        enriched.append({**s,'price_extremo':s['price_to'],'rev_1h':r1,'rev_4h':r4,'rev_24h':r24})
    avg = lambda lst: round(sum(lst)/len(lst),1) if lst else None
    return jsonify({'total':len(signals),'rev_1h_media':avg(revs_1h),'rev_4h_media':avg(revs_4h),'rev_24h_media':avg(revs_24h),'vol_min':round(min(vols),0),'vol_avg':round(sum(vols)/len(vols),0),'avg_diff':round(sum(diffs)/len(diffs),2),'signals':enriched[:50]})

@app.route('/api/winrate')
def api_winrate():
    return jsonify(db_get("""
        SELECT s.pair, s.side, COUNT(*) as total,
               SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) as wins,
               ROUND(100.0*SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END)/COUNT(*),1) as winrate
        FROM signals s JOIN price_tracking pt ON pt.signal_id=s.id
        WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
        GROUP BY s.pair, s.side HAVING total >= 2 ORDER BY winrate DESC, total DESC LIMIT 30
    """))

@app.route('/api/ticker_batch')
def api_ticker_batch():
    pairs = flask_request.args.get('pairs', '')
    if not pairs: return jsonify({})
    result = {}
    for pair in pairs.split(','):
        pair = pair.strip()
        if pair:
            data = get_ticker_24h(pair)
            if data: result[pair] = data
    return jsonify(result)

@app.route('/api/ticker24h')
def api_ticker24h():
    pair = flask_request.args.get('pair', '')
    if not pair: return jsonify({'error': 'no pair'})
    data = get_ticker_24h(pair)
    if not data: return jsonify({'error': 'no data'})
    return jsonify(data)

@app.route('/api/export/<export_type>')
def api_export(export_type):
    from flask import Response
    import io, csv
    if export_type == 'signals':
        data = db_get("SELECT * FROM signals ORDER BY id DESC")
        fields = ['id','timestamp','pair','side','price_from','price_to','price_diff_pct','volume_token','volume_eur','order_type','num_trades']
        filename = 'whale_signals.csv'
    elif export_type == 'mechazos':
        signals = db_get("SELECT * FROM signals ORDER BY id DESC LIMIT 500")
        data = [{**s,'rev_1h':get_reversion(s['id'],60),'rev_4h':get_reversion(s['id'],240),'rev_24h':get_reversion(s['id'],1440),'price_extremo':s['price_to']} for s in signals]
        fields = ['id','timestamp','pair','side','price_diff_pct','price_extremo','volume_eur','num_trades','rev_1h','rev_4h','rev_24h']
        filename = 'whale_mechazos.csv'
    elif export_type == 'winrate':
        data = db_get("""
            SELECT s.pair, s.side, COUNT(*) as total,
                   SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) as wins,
                   ROUND(100.0*SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END)/COUNT(*),1) as winrate
            FROM signals s JOIN price_tracking pt ON pt.signal_id=s.id
            WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
            GROUP BY s.pair, s.side HAVING total >= 2 ORDER BY winrate DESC
        """)
        fields = ['pair','side','total','wins','winrate']
        filename = 'whale_winrate.csv'
    else:
        return jsonify({'error': 'Unknown type'})
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
    writer.writeheader(); writer.writerows(data)
    return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': f'attachment;filename={filename}'})
def run_dashboard():
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

########################### Functions

def getPairs():
    pairsJson = requests.get("https://api.kraken.com/0/public/AssetPairs")
    if pairsJson.status_code != 200:
        telegram_bot_sendtext(f"{datetime.now()} | Error while getting pairs. Code: {pairsJson.status_code}")
    pairs = pairsJson.json()['result']
    return(pairs)

def getEurPairs(wsnames):
    eurPairs = [ls for ls in wsnames if re.findall("/EUR$|^EUR/", ls)]
    return(eurPairs)

def cleanPairs(pairsList):
    for ch in ["'", "[", "]", "/", " "]:
        if(ch in str(pairsList)):
            pairsList = str(pairsList).replace(ch, "")
    return(pairsList)

def getEurPrice(wsnames):
    wsnamesValues = list(wsnames.values())
    eurList = getEurPairs(wsnamesValues)
    cleanEurList = cleanPairs(eurList)
    url = f'https://api.kraken.com/0/public/Ticker?pair={cleanEurList}'
    response = requests.get(url)
    if response.status_code != 200:
        telegram_bot_sendtext(f"{datetime.now()} | Error while getting EUR prices. Code: {response.status_code}")
    rawPrices = response.json()["result"]
    priceEUR = {}
    for k, v in rawPrices.items():
        if re.findall("^EUR", k):
            priceEUR[k.replace("EUR", "") + "EUR"] = round(1/pd.to_numeric(v["c"][0]), 4)
        elif re.findall("^ZEUR", k):
            priceEUR[k.replace("ZEUR", "") + "ZEUR"] = round(1/pd.to_numeric(v["c"][0]), 4)
        else:
            priceEUR[k] = pd.to_numeric(v["c"][0])
    return(priceEUR)

def getNamesForWS(pairs):
    wsnames = {key:value.get('wsname') for (key, value) in pairs.items() if value.get('wsname') is not None}
    return(wsnames)

def filterPairs(wsnames, currency):
    filtered = {k: v for k, v in wsnames.items() if re.findall(f"/{currency}$|^{currency}/", v)}
    if currency == "EUR":
        extras = {k: v for k, v in wsnames.items() if re.findall("/USDT$|/GBP$", v) and k not in filtered}
        filtered.update(extras)
    return(filtered)

def volumeInEUR(wsnames, pair, volume, eurPrices):
    """
    Calcula el volumen de la operación en USD.
    Siempre devuelve USD independientemente de la moneda base del par.
    """
    token = pair.split("/")[0]
    base  = pair.split("/")[1] if "/" in pair else "USD"
    vol   = abs(float(volume))

    # Factores de conversión a USD desde eurPrices
    eur_usd = float(eurPrices.get("ZEURZUSD") or eurPrices.get("EURUSD") or 1.08)
    gbp_usd = 1.27  # fallback GBP

    # Par X/USD o X/USDT — volumen directo en USD
    if base in ("USD", "USDT"):
        # El volumen ya está en tokens, multiplicar por precio
        # price_to es el precio de cierre del trade en USD
        try:
            price = float(pd.to_numeric(pd.Series([0])).iloc[0])  # placeholder
        except:
            pass
        # volumeInEUR recibe volume ya calculado como sum(qty*price) en tradeLoop
        # aquí vol es qty total, necesitamos precio — lo tomamos del último precio del par
        try:
            tokenBase = token + "/USD"
            pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
            price_usd = float(eurPrices.get(pairKey, 0))
            if price_usd > 0:
                return vol * price_usd
        except:
            pass
        return vol  # ya en USD si no encontramos precio

    # Par X/EUR — convertir a USD
    if base == "EUR":
        try:
            tokenBase = token + "/EUR"
            pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
            price_eur = float(eurPrices.get(pairKey, 0))
            if price_eur > 0:
                return vol * price_eur * eur_usd
        except:
            pass
        return vol * eur_usd

    # Par X/GBP — convertir a USD
    if base == "GBP":
        try:
            tokenBase = token + "/GBP"
            pairKey = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
            price_gbp = float(eurPrices.get(pairKey, 0))
            if price_gbp > 0:
                return vol * price_gbp * gbp_usd
        except:
            pass
        return vol * gbp_usd

    # Par USD/X (ej: USD/CHF) — vol es en USD directamente
    if token == "USD":
        return vol

    # ETH2.S especial
    if token == "ETH2.S":
        try:
            pairKey = list(wsnames.keys())[list(wsnames.values()).index("ETH/USD")]
            return vol * float(eurPrices[pairKey]) * 0.96
        except:
            pass

    # Cualquier otro: buscar X/USD → X/EUR→USD → X/USDT
    for quote in ["USD", "USDT"]:
        try:
            pairKey = list(wsnames.keys())[list(wsnames.values()).index(f"{token}/{quote}")]
            price = float(eurPrices.get(pairKey, 0))
            if price > 0:
                return vol * price
        except:
            continue

    try:
        pairKey = list(wsnames.keys())[list(wsnames.values()).index(f"{token}/EUR")]
        price_eur = float(eurPrices.get(pairKey, 0))
        if price_eur > 0:
            return vol * price_eur * eur_usd
    except:
        pass

    return 0

def anotateVolume(x):
    x = pd.to_numeric(x)
    if x <= 0: return(0)
    zeros = int(math.log10(x))
    if(zeros >= 3 and zeros < 6): return("{}K".format(round(x/1e3, 2)))
    elif(zeros >= 6 and zeros < 9): return("{}M".format(round(x/1e6, 2)))
    elif(zeros >= 9): return("{}B".format(round(x/1e9, 2)))
    else: return(x)

def connectToWS(pairsList):
    ws = [None, None]
    try:
        ws[0] = create_connection("wss://ws.kraken.com")
        ws[1] = create_connection("wss://ws.kraken.com")
    except:
        i = 1
        while i < 600:
            print("WS sleeping:", i, "s")
            time.sleep(i)
            i = i + 1
            continue
    chunks = [pairsList[i:i+25] for i in range(0, len(pairsList), 25)]
    for chunk in chunks:
        msg = json.dumps({"event": "subscribe", "pair": chunk, "subscription": {"name": "trade"}})
        ws[0].send(msg)
        ws[1].send(msg)
        time.sleep(2)
    return(ws)

def receiveSafeWS(ws):
    source = 0
    for count in range(100):
        try:
            try:
                return(ws[0].recv())
            except Exception:
                traceback.print_exc()
                source = 1
            try:
                return(ws[1].recv())
            except Exception:
                traceback.print_exc()
                source = 0
        except KeyboardInterrupt:
            print("closing", source)
            ws[0].close()
            ws[1].close()
            print("closed", source)
    WSsource = "Primary" if source == 0 else "Backup"
    print("WebSocket {}):".format(WSsource))

def createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs, ticker=None, change_7d=None, ai_scores=None):
    pairTB = pair.replace("/", "")
    primeraLinea = f"#{pairTB}"
    token = pair.split("/")[0]
    base = pair.split("/")[1]
    tokenNormalized = "BTC" if token == "XBT" else token
    baseNormalized  = "BTC" if base == "XBT" else base
    volumeToken = round(sum(pd.to_numeric(tradeDF["volume"])), 3)
    volumeTokenAnnotated = anotateVolume(volumeToken)
    volumeBase = round(sum(pd.to_numeric(tradeDF["volume"]) * pd.to_numeric(tradeDF["price"])), 3)
    volumeBase = anotateVolume(volumeBase)
    sign = '\U0001F34F' if tradeDF["side"][0] == "b" else '\U0001F34E-'
    whaleSize = max(1, int(math.log10(max(volInEUR, 1000)/1000)+1))
    whaleEmojis = "\U0001F433" * whaleSize
    if(priceDiff >= 5 and priceDiff < 10): changeEmoji = '\U0001F632'
    elif(priceDiff >= 10 and priceDiff < 20): changeEmoji = '\U0001F911'
    elif(priceDiff >= 20): changeEmoji = '\U0001F92F'
    else: changeEmoji = ""
    volInEUR_annotated = anotateVolume(volInEUR)
    # Segunda linea: lado, diff%, ballenas, volumen en USD
    segundaLinea = f"\n*{sign}{round(priceDiff, 2)}%* {changeEmoji} {whaleEmojis}: {volInEUR_annotated} USD ({volumeTokenAnnotated} {token})"
    # Tercera linea: solo enlace a Kraken
    url = f'[{pair}](https://pro.kraken.com/app/trade/{tokenNormalized}-{baseNormalized})'
    terceraLinea = f"\n{url}"
    cuartaLinea = ""
    pairToNonKraken = list(wsnames.keys())[list(wsnames.values()).index(pair)]
    leverage = {key:value.get('leverage_sell') for (key, value) in pairs.items() if value.get('leverage_sell') is not None}
    pairLeverage = leverage[pairToNonKraken]
    if(pairLeverage != []):
        maxLeverage = max(leverage[pairToNonKraken])
        direction = "s" if tradeDF["side"][0] == "b" else "b"
        size = math.floor(volumeToken * 0.1)
        distanceTrade = math.floor(abs(priceDiff))
        distanceTolerance = round(abs(distanceTrade) * 0.15, 2)
        setmaxLeverage = f"{maxLeverage}:1"
        marginMessage = f"\n`/add {tokenNormalized} {baseNormalized} {size} {direction} {distanceTrade} {distanceTolerance} {setmaxLeverage}`"
    else:
        marginMessage = ""
    # Linea 24h + 7d + CMC
    token_clean = tokenNormalized
    cmc_url = get_cmc_url(token_clean)
    if ticker:
        change_emoji_24 = '📈' if ticker['change_24h'] > 0 else '📉'
        base_token = pair.split('/')[1] if '/' in pair else ''
        vol24_annotated = anotateVolume(round(max(ticker['vol_24h_base'], 1), 0))
        if change_7d is not None:
            emoji_7d = '📈' if change_7d > 0 else '📉'
            line_7d = f" | {emoji_7d} 7d: *{change_7d:+.2f}%*"
        else:
            line_7d = ""
        vol7d = ticker.get('vol_7d_usd')
        vol7d_str = f" | Vol 7d: {anotateVolume(vol7d)}$" if vol7d else ""
        quintaLinea = f"\n{change_emoji_24} 24h: *{ticker['change_24h']:+.2f}%* | Vol: {vol24_annotated}${line_7d}{vol7d_str}"
        sextaLinea = f"\n[📊 CoinGecko]({cmc_url})"
    else:
        quintaLinea = ""
        sextaLinea = f"\n[📊 CoinGecko]({cmc_url})"
    # Score IA — aparece después del enlace a CoinGecko
    if ai_scores:
        score_final = ai_scores["score_final"]
        # Emoji de calidad según score
        if score_final >= 8:
            quality = "🟢"
        elif score_final >= 6:
            quality = "🟡"
        else:
            quality = "🔴"
        fund_str = f"{ai_scores['score_fund']:.1f}" if ai_scores.get('score_fund') else "—"
        tec_str  = f"{ai_scores['score_tec']:.1f}"  if ai_scores.get('score_tec')  else "—"
        resumen_fund = ai_scores.get("resumen_fund") or ""
        resumen_tec  = ai_scores.get("resumen_tec")  or ""
        septimaLinea = (
            f"\n{quality} *Score IA: {score_final}/10*"
            f" (Fund: {fund_str} | Téc: {tec_str})"
            f"\n📌 Fund: _{resumen_fund}_"
            f"\n📌 Téc: _{resumen_tec}_"
        )
    else:
        septimaLinea = ""
    return f"{primeraLinea} {segundaLinea} {terceraLinea}{sextaLinea}{quintaLinea}{septimaLinea}{marginMessage}"

def telegram_bot_sendtext(bot_message):
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': bot_chatID,
        'parse_mode': "Markdown",
        'text': bot_message,
        'link_preview_options': {'is_disabled': True}
    }
    response = requests.post(url, json=payload)
    return response.json()


def twilio_llamada(pair, priceDiff, volInEUR):
    """Dispara una llamada de alerta via Twilio cuando hay señal importante."""
    try:
        r = requests.get(
            'https://alertastrading-2306.twil.io/llamada-bot',
            params={'pair': pair, 'diff': priceDiff, 'vol': round(volInEUR, 0)},
            timeout=10
        )
        print(f"📞 Llamada Twilio [{pair}]: {r.status_code}")
    except Exception as e:
        print(f"⚠️ Error Twilio: {e}")

def tradeLoop(pairsList, wsnames, pairs, eurPrices, label):
    print(f"[{label}] Conectando {len(pairsList)} pares...")
    ws = connectToWS(pairsList)
    print(f"[{label}] Subscrito y recibiendo...")
    eurPrices_lock = threading.Lock()
    local_eurPrices = eurPrices.copy()
    while True:
        try:
            firstReceived = receiveSafeWS(ws)
            if firstReceived is None:
                raise Exception(f"[{label}] receiveSafeWS returned None")
            result = list(json.loads(firstReceived))
            while type(result[0]) != int:
                firstReceived = receiveSafeWS(ws)
                if firstReceived is None: raise Exception(f"[{label}] None")
                result = list(json.loads(firstReceived))
            while len(result) == 1:
                firstReceived = receiveSafeWS(ws)
                if firstReceived is None: raise Exception(f"[{label}] None")
                result = list(json.loads(firstReceived))
            if len(result[1]) != 1:
                tradeDF = pd.DataFrame(result[1], columns=["price", "volume", "time", "side", "orderType", "misc"])
                tradeDF = tradeDF.sort_values(by=["time"])
                prices = pd.to_numeric(tradeDF["price"])
                priceDiff = abs(float((prices.iloc[0] - prices.iloc[-1]) * 100 / prices.iloc[0]))
                pair = result[3]
                volume = sum(pd.to_numeric(tradeDF["volume"]))
                # Volumen en USD = suma(cantidad × precio) para cada trade
                # Esto da el volumen en la moneda base del par
                vol_base = float(sum(
                    pd.to_numeric(tradeDF["volume"]) * pd.to_numeric(tradeDF["price"])
                ))
                base = pair.split("/")[1] if "/" in pair else "USD"
                token_sym = pair.split("/")[0] if "/" in pair else pair
                eur_usd = float(local_eurPrices.get("ZEURZUSD") or local_eurPrices.get("EURUSD") or 1.08)
                # Conversión a USD según moneda base del par
                if base in ("USD", "USDT"):
                    volInEUR = vol_base                  # ya en USD
                elif base == "EUR":
                    volInEUR = vol_base * eur_usd        # EUR → USD
                elif base == "GBP":
                    # Obtener GBP/USD de eurPrices si existe, sino fallback
                    try:
                        gbp_key = list(local_eurPrices.keys())[
                            [k for k in local_eurPrices.keys() if 'GBP' in k and 'USD' in k].__getitem__(0)
                            if [k for k in local_eurPrices.keys() if 'GBP' in k and 'USD' in k] else 0
                        ]
                        gbp_usd = float(local_eurPrices[gbp_key])
                    except:
                        gbp_usd = 1.27
                    volInEUR = vol_base * gbp_usd        # GBP → USD
                elif base == "XBT" or base == "BTC":
                    # Par pagado en BTC — convertir BTC a USD
                    try:
                        btc_key = list(local_eurPrices.keys())[
                            next(i for i,k in enumerate(local_eurPrices.keys()) if 'XBT' in k and 'USD' in k)
                        ]
                        btc_usd = float(local_eurPrices[btc_key])
                        volInEUR = vol_base * btc_usd
                    except:
                        volInEUR = vol_base * 60000  # fallback BTC precio
                elif base == "ETH":
                    try:
                        eth_key = next(k for k in local_eurPrices.keys() if 'ETH' in k and 'USD' in k)
                        eth_usd = float(local_eurPrices[eth_key])
                        volInEUR = vol_base * eth_usd
                    except:
                        volInEUR = vol_base * 3000  # fallback ETH precio
                else:
                    # Para cualquier otra moneda base: intentar convertir via EUR/USD
                    volInEUR = vol_base * eur_usd
                if(priceDiff > 2 and volInEUR > 15000):  # volInEUR es realmente vol en USD equivalente
                    priceDiff = round(priceDiff, 3)
                    print(f"\U0001F433 [{label}]", priceDiff, pair)
                    entry_price = float(tradeDF["price"].iloc[-1])
                    # Guardar siempre en BD para análisis
                    signal_id = save_signal(tradeDF, pair, volInEUR, priceDiff)
                    launch_tracker(signal_id, pair, entry_price)
                    # Guardar siempre — enviar a TG solo si vol 24h > 50K$
                    ticker = get_ticker_24h(pair)
                    vol_24h = ticker['vol_24h_base'] if ticker else None
                    if vol_24h is None:
                        # No se pudo obtener el volumen — omitir por seguridad
                        print(f"⏭ [{label}] {pair} omitido — no se pudo obtener vol 24h")
                    elif vol_24h < 50000:
                        print(f"⏭ [{label}] {pair} omitido — vol 24h: {round(vol_24h,0):,.0f}$ < 50K$")
                    else:
                        # Obtener 7d del cache
                        _tok = pair.split("/")[0] if "/" in pair else pair
                        for _s in [".S",".P",".M","2"]: _tok = _tok.replace(_s,"")
                        c7d = _cg_cache.get("7d_" + _tok)
                        # 1. Enviar señal inmediatamente sin score IA
                        TGmsg = createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs, ticker, c7d, None)
                        tg_response = telegram_bot_sendtext(TGmsg)
                        msg_id = tg_response.get("result", {}).get("message_id")

                        # 2. Calcular score IA en background y enviar como reply
                        def send_ai_score_reply(pair=pair, priceDiff=priceDiff, volInEUR=volInEUR,
                                                side=tradeDF["side"].iloc[0], ticker=ticker,
                                                c7d=c7d, msg_id=msg_id):
                            if not ANTHROPIC_KEY:
                                return
                            try:
                                scores = get_ai_scores(pair, priceDiff, volInEUR, side, ticker, c7d)
                                if not scores:
                                    return
                                score_final = scores["score_final"]
                                if score_final >= 8:
                                    quality = "🟢"
                                elif score_final >= 6:
                                    quality = "🟡"
                                else:
                                    quality = "🔴"
                                fund_str = f"{scores['score_fund']:.1f}" if scores.get('score_fund') else "—"
                                tec_str  = f"{scores['score_tec']:.1f}"  if scores.get('score_tec')  else "—"
                                rf = scores.get("resumen_fund","")
                                rt = scores.get("resumen_tec","")
                                score_msg = (
                                    f"{quality} *Score IA: {score_final}/10*"
                                    f" (Fund: {fund_str} | Téc: {tec_str})"
                                    f"\n📌 Fund: _{rf}_"
                                    f"\n📌 Téc: _{rt}_"
                                )
                                # Enviar como reply al mensaje original
                                url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                                params = {
                                    "chat_id": bot_chatID,
                                    "parse_mode": "Markdown",
                                    "text": score_msg,
                                    "disable_web_page_preview": True
                                }
                                if msg_id:
                                    params["reply_to_message_id"] = msg_id
                                requests.post(url, params=params, timeout=10)
                                print(f"✅ Score IA enviado: {pair} {score_final}/10")
                            except Exception as _e:
                                print(f"AI score reply error: {_e}")

                        threading.Thread(target=send_ai_score_reply, daemon=True).start()
                        # Actualizar cache 7d en hilo separado
                        threading.Thread(target=get_7d_change, args=(pair,), daemon=True).start()
                        # Actualizar contador de señales recientes y disparar Twilio si aplica
                        now_ts = datetime.utcnow()
                        with _recent_lock:
                            _recent_signals[pair].append(now_ts)
                            # Limpiar señales fuera de la ventana
                            cutoff = now_ts - __import__('datetime').timedelta(minutes=RAFAGA_WINDOW_MIN)
                            _recent_signals[pair] = [t for t in _recent_signals[pair] if t > cutoff]
                            count_recent = len(_recent_signals[pair])
                        print(f"📊 [{label}] {pair} — {count_recent} señales en {RAFAGA_WINDOW_MIN}min")
                        # Llamada solo si 2+ señales en 7 días Y score >= 7
                        if count_recent >= RAFAGA_MIN_COUNT:
                            # Verificar score del último análisis
                            last_score = None
                            fund_cached = _ai_score_cache.get(f"fund_{pair}")
                            tec_key = next((k for k in _ai_score_cache if k.startswith(f"tec_{pair}_")), None)
                            tec_cached = _ai_score_cache.get(tec_key) if tec_key else None
                            if fund_cached and tec_cached:
                                last_score = round((fund_cached["score"] + tec_cached["score"]) / 2, 1)
                            elif tec_cached:
                                last_score = tec_cached["score"]
                            elif fund_cached:
                                last_score = fund_cached["score"]
                            print(f"📊 Twilio check: {pair} | señales 7d: {count_recent} | score: {last_score}")
                            if last_score is None or last_score >= 7:
                                threading.Thread(
                                    target=twilio_llamada,
                                    args=(pair, priceDiff, volInEUR),
                                    daemon=True
                                ).start()
                else:
                    print(".", end="", flush=True)
            if(datetime.now().second == 0):
                time.sleep(1)
                with eurPrices_lock:
                    local_eurPrices = getEurPrice(wsnames)
                print(f"✓[{label}] ", end="")
        except Exception:
            traceback.print_exc()
            print(f"[{label}] Disconnected. Reconnecting in 5s...")
            time.sleep(5)
            try:
                ws = connectToWS(pairsList)
            except Exception:
                time.sleep(30)

def connectTradeWS():
    init_db()

    # Arrancar dashboard en hilo separado
    t_dash = threading.Thread(target=run_dashboard, daemon=True)
    t_dash.start()
    print("🌐 Dashboard arrancado en puerto 5000")

    print("Getting pairs...")
    pairs = getPairs()
    wsnames = getNamesForWS(pairs)
    print(f"Total pairs: {len(wsnames)}")
    eurPrices = getEurPrice(wsnames)
    eurWsnames = filterPairs(wsnames, "EUR")
    usdWsnames = filterPairs(wsnames, "USD")
    usdOnlyWsnames = {k: v for k, v in usdWsnames.items() if k not in eurWsnames}
    eurList = list(eurWsnames.values())
    usdList = list(usdOnlyWsnames.values())
    print(f"EUR pairs: {len(eurList)} | USD-only pairs: {len(usdList)}")
    telegram_bot_sendtext(f"\U0001F40D Levantado — EUR: {len(eurList)} | USD: {len(usdList)} pares")

    t_eur = threading.Thread(target=tradeLoop, args=(eurList, wsnames, pairs, eurPrices, "EUR"), daemon=True)
    t_eur.start()
    print("Hilo EUR arrancado")
    time.sleep(60)
    t_usd = threading.Thread(target=tradeLoop, args=(usdList, wsnames, pairs, eurPrices, "USD"), daemon=True)
    t_usd.start()
    print("Hilo USD arrancado")

    while True:
        time.sleep(60)
        if not t_eur.is_alive():
            print("⚠️ Hilo EUR muerto — reiniciando...")
            t_eur = threading.Thread(target=tradeLoop, args=(eurList, wsnames, pairs, eurPrices, "EUR"), daemon=True)
            t_eur.start()
        if not t_usd.is_alive():
            print("⚠️ Hilo USD muerto — reiniciando...")
            t_usd = threading.Thread(target=tradeLoop, args=(usdList, wsnames, pairs, eurPrices, "USD"), daemon=True)
            t_usd.start()

connectTradeWS()
