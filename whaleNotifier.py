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

# Base de datos
DB_PATH = os.getenv("DB_PATH", "/data/signals.db")

# ─────────────────────────────────────────────
# BASE DE DATOS
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
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

def save_signal(tradeDF, pair, volInEUR, priceDiff):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_PATH)
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
                conn = sqlite3.connect(DB_PATH)
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
.tabs{display:flex;border-bottom:1px solid var(--border);padding:0 1.5rem;overflow-x:auto;gap:0}
.tab{padding:0.65rem 1rem;cursor:pointer;font-size:0.72rem;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap;transition:all 0.2s}
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

/* ── RAFAGAS ── */
.rafaga-wrap{padding:1rem 1.5rem;display:flex;flex-direction:column;gap:1rem}
.rafaga-card{background:var(--surface);border:2px solid var(--orange);border-radius:12px;overflow:hidden;animation:pulse-border 2s infinite}
.rafaga-card.caliente{border-color:var(--red)}
.rafaga-card.tibia{border-color:var(--orange)}
.rafaga-card.fria{border-color:var(--border)}
@keyframes pulse-border{0%,100%{box-shadow:0 0 0 0 transparent}50%{box-shadow:0 0 12px 2px rgba(255,170,0,0.3)}}
.rafaga-card.caliente{animation:pulse-border-red 2s infinite}
@keyframes pulse-border-red{0%,100%{box-shadow:0 0 0 0 transparent}50%{box-shadow:0 0 16px 4px rgba(255,68,102,0.4)}}
.rafaga-header{display:flex;align-items:center;gap:0.75rem;padding:0.85rem 1rem;border-bottom:1px solid var(--border);flex-wrap:wrap}
.rafaga-pair{font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:800;color:var(--accent)}
.rafaga-badge{font-size:0.65rem;font-weight:700;padding:0.2rem 0.55rem;border-radius:20px;font-family:'Syne',sans-serif}
.badge-hot{background:#ff446620;color:var(--red);border:1px solid var(--red)}
.badge-warm{background:#ffaa0020;color:var(--orange);border:1px solid var(--orange)}
.badge-cool{background:#00d4ff10;color:var(--muted);border:1px solid var(--muted)}
.rafaga-meta{display:flex;gap:1.5rem;flex-wrap:wrap;font-size:0.68rem}
.rafaga-meta span{color:var(--muted)}
.rafaga-meta strong{color:var(--text)}
.nivel-key{display:inline-block;background:#001830;border:1px solid var(--green);border-radius:4px;padding:0.15rem 0.5rem;font-size:0.7rem;color:var(--green);font-weight:700;margin-left:0.5rem}
.nivel-key.sell{border-color:var(--red);color:var(--red)}
.rafaga-signals{padding:0.75rem 1rem;display:flex;flex-direction:column;gap:0.4rem}
.rafaga-signal-row{display:grid;grid-template-columns:80px 60px 90px 80px 1fr;gap:0.5rem;align-items:center;font-size:0.68rem;padding:0.3rem 0.5rem;border-radius:6px;background:#080c14}
.rafaga-signal-row:hover{background:var(--surface)}
.rafaga-footer{padding:0.65rem 1rem;border-top:1px solid var(--border);display:flex;gap:1rem;align-items:center;flex-wrap:wrap}
.sugerencia{font-size:0.68rem;background:#001830;border:1px solid var(--accent);border-radius:6px;padding:0.4rem 0.75rem;color:var(--accent)}
.tiempo-hace{font-size:0.62rem;color:var(--muted)}
.nivel-recurrente{font-size:0.7rem;color:var(--purple);font-weight:700}

/* CANDIDATOS */
.candidates-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:0.9rem;padding:1.2rem 1.5rem}
.candidate-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem;cursor:pointer;transition:border-color 0.2s}
.candidate-card:hover{border-color:var(--accent)}
.cand-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem}
.cand-pair{font-family:'Syne',sans-serif;font-size:0.95rem;font-weight:800;color:var(--accent)}
.score-badge{font-family:'Syne',sans-serif;font-size:0.75rem;font-weight:800;padding:0.18rem 0.55rem;border-radius:20px}
.score-high{background:#00ff8820;color:var(--green);border:1px solid var(--green)}
.score-med{background:#ffaa0020;color:var(--orange);border:1px solid var(--orange)}
.score-low{background:#ff446620;color:var(--red);border:1px solid var(--red)}
.cand-metrics{display:grid;grid-template-columns:1fr 1fr;gap:0.4rem}
.cand-metric{background:#0a1220;border-radius:6px;padding:0.45rem 0.6rem}
.cand-metric-label{font-size:0.56rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.07em}
.cand-metric-value{font-size:0.85rem;font-weight:700;font-family:'Syne',sans-serif;margin-top:0.1rem}

/* POR PAR */
.par-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:0.6rem;margin-bottom:1.2rem}
.rev-grid{display:grid;grid-template-columns:55px 1fr 48px;gap:0.35rem;align-items:center;margin-bottom:0.35rem;font-size:0.66rem}
.rev-bar-wrap{background:#0f1a28;border-radius:4px;height:6px;overflow:hidden}
.rev-bar{height:100%;border-radius:4px}

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

/* EXPORT */
.export-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:0.9rem;padding:1.2rem 1.5rem}
.export-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.9rem}
.export-card p{font-size:0.65rem;color:var(--muted);margin:0.4rem 0 0.7rem;line-height:1.5}
.sec-title{font-family:'Syne',sans-serif;font-size:0.82rem;font-weight:700;color:var(--accent);padding:1rem 1.5rem 0.5rem}
.sec-sub{font-size:0.65rem;color:var(--muted);padding:0 1.5rem 0.75rem}
.chip-wrap{display:flex;flex-wrap:wrap;gap:0.3rem;padding:0.6rem 1.5rem;border-bottom:1px solid var(--border)}
.chip{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:0.15rem 0.45rem;font-size:0.65rem;cursor:pointer;transition:all 0.15s}
.chip:hover{border-color:var(--accent);color:var(--accent)}
.chip .cnt{color:var(--muted);margin-left:0.2rem}
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
  <div class="tab active" onclick="switchTab('rafagas')">🔥 Ráfagas</div>
  <div class="tab" onclick="switchTab('candidates')">🎯 Candidatos</div>
  <div class="tab" onclick="switchTab('par')">🔍 Por par</div>
  <div class="tab" onclick="switchTab('signals')">📋 Señales</div>
  <div class="tab" onclick="switchTab('export')">📥 Exportar</div>
</div>

<!-- TAB RÁFAGAS -->
<div class="tab-content active" id="tab-rafagas">
  <div class="sec-title">🔥 Ráfagas de ballena</div>
  <div class="sec-sub">Pares con múltiples señales en poco tiempo. Rojo = activo ahora mismo. La clave es el nivel de precio repetido — ahí está la orden.</div>
  <div class="controls">
    <select id="rafaga-window" onchange="loadRafagas()">
      <option value="5">Últimos 5 min</option>
      <option value="15" selected>Últimos 15 min</option>
      <option value="30">Últimos 30 min</option>
      <option value="60">Última hora</option>
      <option value="240">Últimas 4h</option>
      <option value="1440">Últimas 24h</option>
    </select>
    <select id="rafaga-min" onchange="loadRafagas()">
      <option value="2">Min 2 señales</option>
      <option value="3" selected>Min 3 señales</option>
      <option value="4">Min 4 señales</option>
    </select>
    <button onclick="loadRafagas()">Filtrar</button>
  </div>
  <div class="rafaga-wrap" id="rafaga-wrap">
    <div class="no-data">Cargando ráfagas...</div>
  </div>
</div>

<!-- TAB CANDIDATOS -->
<div class="tab-content" id="tab-candidates">
  <div class="sec-title">🎯 Pares candidatos</div>
  <div class="sec-sub">Score basado en % reversión × frecuencia × tamaño. Clic en una tarjeta para ver el historial completo.</div>
  <div class="candidates-grid" id="candidates-grid">
    <div class="no-data">Cargando...</div>
  </div>
</div>

<!-- TAB POR PAR -->
<div class="tab-content" id="tab-par">
  <div class="sec-title">🔍 Análisis por par</div>
  <div class="sec-sub">Historial de mechazos, % reversión por intervalo y niveles clave de precio.</div>
  <div class="controls">
    <input type="text" id="par-input" placeholder="Ej: ADA/GBP" style="width:140px">
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
      <thead><tr><th>#</th><th>Fecha</th><th>Par</th><th>Lado</th><th>Diff %</th><th>Precio entrada</th><th>Precio extremo</th><th>Vol EUR</th><th>Chart</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- TAB EXPORTAR -->
<div class="tab-content" id="tab-export">
  <div class="sec-title">📥 Exportar para análisis IA</div>
  <div class="sec-sub">Descarga los datos en CSV para analizar con Claude u otras herramientas.</div>
  <div class="export-grid">
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Todas las señales</div><p>Cada señal con par, lado, precio, volumen y timestamp.</p><button class="btn-export" onclick="exportCSV('signals')">⬇ Descargar</button></div>
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Ráfagas</div><p>Grupos de señales con nivel clave y sugerencia de orden.</p><button class="btn-export" onclick="exportCSV('rafagas')">⬇ Descargar</button></div>
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Mechazos + reversión</div><p>Señales con % reversión a 1h, 4h y 24h.</p><button class="btn-export" onclick="exportCSV('mechazos')">⬇ Descargar</button></div>
    <div class="export-card"><div style="color:var(--accent);font-family:'Syne',sans-serif;font-size:0.78rem;font-weight:700">Win Rate por par</div><p>Ranking de pares por % señales con resultado positivo.</p><button class="btn-export" onclick="exportCSV('winrate')">⬇ Descargar</button></div>
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
function scoreClass(s){return s>=7?'score-high':s>=4?'score-med':'score-low';}
function timeAgo(ts){
  const diff=Math.floor((Date.now()-new Date(ts).getTime())/1000);
  if(diff<60)return diff+'s';
  if(diff<3600)return Math.floor(diff/60)+'min';
  return Math.floor(diff/3600)+'h';
}

function switchTab(tab){
  const tabs=['rafagas','candidates','par','signals','export'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===tab));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  if(tab==='rafagas') loadRafagas();
  if(tab==='candidates') loadCandidates();
  if(tab==='signals') loadSignals();
}

async function loadAll(){
  await Promise.all([loadStats(), loadRafagas()]);
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
    <div class="stat"><div class="stat-label">Vol medio €</div><div class="stat-value">${fmt(s.avg_vol)}</div></div>`;
  document.getElementById('topPairs').innerHTML=s.top_pairs.map(p=>
    `<div class="chip" onclick="goToPar('${p.pair}')">${p.pair}<span class="cnt">${p.count}</span></div>`).join('');
}

function goToPar(pair){
  switchTab('par');
  document.getElementById('par-input').value=pair;
  loadPar();
}

async function loadRafagas(){
  const window_min = document.getElementById('rafaga-window').value;
  const min_signals = document.getElementById('rafaga-min').value;
  const data = await(await fetch(`/api/rafagas?window=${window_min}&min=${min_signals}`)).json();
  const el = document.getElementById('rafaga-wrap');
  if(!data||data.length===0){
    el.innerHTML='<div class="no-data">No hay ráfagas en este periodo.<br><br>Prueba ampliar la ventana de tiempo o reducir el mínimo de señales.</div>';
    return;
  }
  el.innerHTML = data.map(r => {
    const minAgo = Math.floor((Date.now()-new Date(r.last_signal).getTime())/60000);
    const isHot  = minAgo < 5;
    const isWarm = minAgo < 30;
    const cardClass = isHot?'caliente':isWarm?'tibia':'fria';
    const badgeClass = isHot?'badge-hot':isWarm?'badge-warm':'badge-cool';
    const badgeText = isHot?'🔴 AHORA':isWarm?'🟡 RECIENTE':'⚪ ANTIGUO';
    const sideLabel = r.dominant_side==='b'?'🍏 Compras ballena':'🍎 Ventas ballena';
    const nivelClass = r.dominant_side==='b'?'':'sell';
    const orderSugg = r.dominant_side==='b'
      ? `VENTA límite en ${r.nivel_clave} (si crees que revierte)`
      : `COMPRA límite en ${r.nivel_clave} (si crees que revierte)`;
    const rows = (r.signals||[]).map(s=>`
      <div class="rafaga-signal-row">
        <span class="muted">${s.timestamp.replace('T',' ').substring(11,19)}</span>
        <span class="${s.side==='b'?'buy':'sell'}">${s.side==='b'?'🍏 BUY':'🍎 SELL'}</span>
        <span class="neu">${s.price_diff_pct>0?'+':''}${s.price_diff_pct}%</span>
        <span class="vol">${parseFloat(s.price_to).toPrecision(5)}</span>
        <span class="vol">${fmt(s.volume_eur)}€</span>
      </div>`).join('');
    return `<div class="rafaga-card ${cardClass}">
      <div class="rafaga-header">
        <span class="rafaga-pair">${r.pair}</span>
        <span class="rafaga-badge ${badgeClass}">${badgeText}</span>
        <span class="rafaga-badge" style="background:#00d4ff10;color:var(--accent);border:1px solid var(--accent)">${r.count} señales</span>
        <span style="font-size:0.65rem;color:var(--muted)">hace ${timeAgo(r.last_signal)}</span>
      </div>
      <div class="rafaga-meta" style="padding:0.6rem 1rem">
        <span>${sideLabel}</span>
        <span>Diff media: <strong>${r.avg_diff}%</strong></span>
        <span>Vol total: <strong class="vol">${fmt(r.total_vol)}€</strong></span>
        <span>Duración: <strong>${r.duracion_min}min</strong></span>
        <span>Nivel clave: <strong class="nivel-clave ${nivelClass}" style="color:${r.dominant_side==='b'?'var(--green)':'var(--red)'}">${r.nivel_clave}</strong></span>
        ${r.nivel_recurrente?`<span>Nivel recurrente: <span class="nivel-recurrente">⭐ ${r.nivel_recurrente}</span></span>`:''}
      </div>
      <div class="rafaga-signals">${rows}</div>
      <div class="rafaga-footer">
        <div class="sugerencia">💡 ${orderSugg}</div>
        <button class="btn-sm" onclick="goToPar('${r.pair}')">🔍 Ver historial</button>
      </div>
    </div>`;
  }).join('');
}

async function loadCandidates(){
  const data=await(await fetch('/api/candidates')).json();
  if(!data||data.length===0){
    document.getElementById('candidates-grid').innerHTML='<div class="no-data" style="grid-column:1/-1">Necesitas señales con tracking de 24h completado para ver candidatos.</div>';
    return;
  }
  document.getElementById('candidates-grid').innerHTML=data.map(d=>{
    const sc=parseFloat(d.score);
    return `<div class="candidate-card" onclick="goToPar('${d.pair}')">
      <div class="cand-header">
        <div>
          <div class="cand-pair">${d.pair}</div>
          <div style="font-size:0.6rem;color:var(--muted);margin-top:0.1rem">${d.side==='b'?'🍏 Compras':'🍎 Ventas'} ballena</div>
        </div>
        <div class="score-badge ${scoreClass(sc)}">Score ${sc}</div>
      </div>
      <div class="cand-metrics">
        <div class="cand-metric"><div class="cand-metric-label">% Reversión media</div><div class="cand-metric-value ${d.avg_reversion>50?'pos':'neg'}">${d.avg_reversion}%</div></div>
        <div class="cand-metric"><div class="cand-metric-label">Frecuencia</div><div class="cand-metric-value" style="color:var(--accent)">${d.frequency}x</div></div>
        <div class="cand-metric"><div class="cand-metric-label">Vol mínimo</div><div class="cand-metric-value vol">${fmt(d.min_vol)}€</div></div>
        <div class="cand-metric"><div class="cand-metric-label">Diff media</div><div class="cand-metric-value neu">${d.avg_diff}%</div></div>
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
  const r1c=data.rev_1h_media>50?'pos':'neg';
  const r4c=data.rev_4h_media>50?'pos':'neg';
  const r24c=data.rev_24h_media>50?'pos':'neg';
  el.innerHTML=`
    <div class="par-metrics">
      <div class="cand-metric"><div class="cand-metric-label">Total señales</div><div class="cand-metric-value" style="color:var(--accent)">${data.total}</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 1h</div><div class="cand-metric-value ${r1c}">${data.rev_1h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 4h</div><div class="cand-metric-value ${r4c}">${data.rev_4h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 24h</div><div class="cand-metric-value ${r24c}">${data.rev_24h_media??'—'}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol mínimo</div><div class="cand-metric-value vol">${fmt(data.vol_min)}€</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol medio</div><div class="cand-metric-value vol">${fmt(data.vol_avg)}€</div></div>
    </div>
    <div style="font-size:0.65rem;color:var(--muted);margin-bottom:0.6rem">REVERSIÓN POR INTERVALO</div>
    <div style="max-width:380px;margin-bottom:1.2rem">
      <div class="rev-grid"><span class="muted">1h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_1h_media||0,100)}%;background:${(data.rev_1h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r1c}">${data.rev_1h_media??'—'}%</span></div>
      <div class="rev-grid"><span class="muted">4h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_4h_media||0,100)}%;background:${(data.rev_4h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r4c}">${data.rev_4h_media??'—'}%</span></div>
      <div class="rev-grid"><span class="muted">24h</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_24h_media||0,100)}%;background:${(data.rev_24h_media||0)>50?'var(--green)':'var(--red)'}"></div></div><span class="${r24c}">${data.rev_24h_media??'—'}%</span></div>
    </div>
    <div style="font-size:0.65rem;color:var(--muted);margin-bottom:0.6rem">MECHAZOS HISTÓRICOS</div>
    <div class="table-wrap"><table>
      <thead><tr><th>#</th><th>Fecha</th><th>Lado</th><th>Diff %</th><th>Precio extremo</th><th>Vol €</th><th>Rev 1h</th><th>Rev 4h</th><th>Rev 24h</th><th>Chart</th></tr></thead>
      <tbody>${(data.signals||[]).map(s=>`<tr>
        <td class="muted">#${s.id}</td>
        <td class="muted" style="font-size:0.65rem">${s.timestamp.replace('T',' ').substring(0,16)}</td>
        <td>${s.side==='b'?'<span class="buy">🍏</span>':'<span class="sell">🍎</span>'}</td>
        <td>${pct(s.price_diff_pct)}</td>
        <td class="vol">${parseFloat(s.price_extremo).toPrecision(5)}</td>
        <td class="vol">${fmt(s.volume_eur)}€</td>
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
    <td><span class="pair-tag" onclick="goToPar('${s.pair}')">${s.pair}</span></td>
    <td>${s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>'}</td>
    <td>${pct(s.price_diff_pct)}</td>
    <td class="vol" style="font-size:0.68rem">${parseFloat(s.price_from).toPrecision(5)}</td>
    <td class="vol" style="font-size:0.68rem">${parseFloat(s.price_to).toPrecision(5)}</td>
    <td class="vol">${fmt(s.volume_eur)}€</td>
    <td><button class="btn-sm" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_to})">📈</button></td>
  </tr>`).join('')||'<tr><td colspan="9" class="no-data">No hay señales</td></tr>';
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
  document.getElementById('modal-meta').textContent=`${sig.side==='b'?'🍏 BUY':'🍎 SELL'} | Entrada: ${parseFloat(sig.price_to).toPrecision(5)} | Vol: ${fmt(sig.volume_eur)}€ | ${sig.timestamp.substring(0,19)}`;
  if(!pts||pts.length===0){document.getElementById('no-data').style.display='block';document.getElementById('priceChart').style.display='none';return;}
  const color=pts[pts.length-1].pct_change>=0?'#00ff88':'#ff4466';
  chartInstance=new Chart(document.getElementById('priceChart'),{type:'line',data:{labels:pts.map(p=>`+${p.minutes}min`),datasets:[{label:'Precio',data:pts.map(p=>p.price),borderColor:color,backgroundColor:color+'15',borderWidth:2,pointRadius:0,tension:0.3,fill:true,yAxisID:'y'},{label:'% cambio',data:pts.map(p=>p.pct_change),borderColor:'#00d4ff',borderWidth:1,borderDash:[4,4],pointRadius:0,tension:0.3,fill:false,yAxisID:'y2'}]},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},plugins:{legend:{labels:{color:'#4a6080',font:{family:'Space Mono',size:10}}},tooltip:{backgroundColor:'#0d1420',borderColor:'#1a2535',borderWidth:1,titleColor:'#c8d8e8',bodyColor:'#c8d8e8'}},scales:{x:{ticks:{color:'#4a6080',font:{size:9},maxTicksLimit:12},grid:{color:'#0f1a28'}},y:{ticks:{color:'#00ff88',font:{size:9}},grid:{color:'#0f1a28'},position:'left'},y2:{ticks:{color:'#00d4ff',font:{size:9}},grid:{display:false},position:'right'}}}});
}
function closeModal(){document.getElementById('modal').classList.remove('open');if(chartInstance){chartInstance.destroy();chartInstance=null;}}
document.getElementById('modal').addEventListener('click',e=>{if(e.target===document.getElementById('modal'))closeModal();});
function exportCSV(type){window.location.href='/api/export/'+type;}
loadAll();
setInterval(loadAll,30000);
</script>
</body>
</html>
"""

def db_get(query, params=[]):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def db_one(query, params=[]):
    conn = sqlite3.connect(DB_PATH)
    val = conn.execute(query, params).fetchone()[0]
    conn.close()
    return val

def get_reversion(signal_id, minutes_target):
    conn = sqlite3.connect(DB_PATH)
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

@app.route('/api/rafagas')
def api_rafagas():
    from datetime import datetime, timedelta
    window_min = int(flask_request.args.get('window', 15))
    min_signals = int(flask_request.args.get('min', 3))
    since = (datetime.utcnow() - timedelta(minutes=window_min)).isoformat()
    signals = db_get("""
        SELECT * FROM signals
        WHERE timestamp >= ?
        ORDER BY pair, timestamp ASC
    """, [since])
    if not signals:
        return jsonify([])
    # Agrupar por par
    from collections import defaultdict
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
        prices_to = [float(s['price_to']) for s in sigs]
        # Nivel clave: precio extremo más repetido (redondeado a 4 decimales)
        rounded = [round(p, 4) for p in prices_to]
        from collections import Counter
        cnt = Counter(rounded)
        nivel_clave = cnt.most_common(1)[0][0]
        nivel_recurrente = cnt.most_common(1)[0][0] if cnt.most_common(1)[0][1] > 1 else None
        # Duracion en minutos
        t_first = sigs[0]['timestamp']
        t_last  = sigs[-1]['timestamp']
        try:
            dur = max(1, int((datetime.fromisoformat(t_last) - datetime.fromisoformat(t_first)).total_seconds() / 60))
        except:
            dur = 1
        result.append({
            'pair': pair,
            'count': len(sigs),
            'dominant_side': dominant_side,
            'avg_diff': round(sum(diffs)/len(diffs), 2),
            'total_vol': round(sum(vols), 0),
            'nivel_clave': nivel_clave,
            'nivel_recurrente': nivel_recurrente,
            'duracion_min': dur,
            'last_signal': t_last,
            'signals': sigs
        })
    # Ordenar: primero más recientes
    result.sort(key=lambda x: x['last_signal'], reverse=True)
    return jsonify(result)

@app.route('/api/candidates')
def api_candidates():
    import math as _math
    pairs_data = db_get("""
        SELECT DISTINCT s.pair, s.side
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        GROUP BY s.pair, s.side HAVING COUNT(DISTINCT s.id) >= 2
    """)
    result = []
    for pd_row in pairs_data:
        pair = pd_row['pair']; side = pd_row['side']
        signals = db_get("SELECT * FROM signals WHERE pair=? AND side=? ORDER BY id DESC", [pair, side])
        revs_24h=[]; vols=[]; diffs=[]
        for s in signals:
            r = get_reversion(s['id'], 1440)
            if r is not None: revs_24h.append(r)
            vols.append(s['volume_eur']); diffs.append(s['price_diff_pct'])
        if not revs_24h: continue
        avg_rev = round(abs(sum(revs_24h)/len(revs_24h)), 1)
        frequency = len(signals)
        min_vol = round(min(vols), 0)
        avg_diff = round(sum(diffs)/len(diffs), 2)
        avg_vol = round(sum(vols)/len(vols), 0) if vols else 0
        # vol_factor: escala logaritmicamente. 1000€=1.0, 10K€=2.0, 100K€=3.0, 1M€=4.0
        vol_factor = _math.log10(max(avg_vol, 100) / 1000 + 1) * 3 + 1
        score = round(avg_rev/10 * _math.log(frequency+1) * (avg_diff/2+1) * vol_factor, 1)
        result.append({'pair':pair,'side':side,'avg_reversion':avg_rev,'frequency':frequency,'min_vol':min_vol,'avg_vol':avg_vol,'avg_diff':avg_diff,'score':score})
    result.sort(key=lambda x: x['score'], reverse=True)
    return jsonify(result[:20])

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

@app.route('/api/mechazos')
def api_mechazos():
    pair = flask_request.args.get('pair', '')
    side = flask_request.args.get('side', '')
    q = "SELECT * FROM signals WHERE 1=1"
    p = []
    if pair: q += " AND pair LIKE ?"; p.append(f"%{pair}%")
    if side: q += " AND side = ?"; p.append(side)
    q += " ORDER BY id DESC LIMIT 200"
    signals = db_get(q, p)
    result = []
    for s in signals:
        result.append({**s,'price_extremo':s['price_to'],'rev_1h':get_reversion(s['id'],60),'rev_4h':get_reversion(s['id'],240),'rev_24h':get_reversion(s['id'],1440)})
    return jsonify(result)

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
    elif export_type == 'rafagas':
        from datetime import datetime, timedelta
        from collections import defaultdict, Counter
        signals = db_get("SELECT * FROM signals ORDER BY pair, timestamp ASC")
        by_pair = defaultdict(list)
        for s in signals: by_pair[s['pair']].append(s)
        data = []
        for pair, sigs in by_pair.items():
            if len(sigs) < 2: continue
            sides = [s['side'] for s in sigs]; dom = max(set(sides), key=sides.count)
            prices = [round(float(s['price_to']),4) for s in sigs]
            nivel = Counter(prices).most_common(1)[0][0]
            data.append({'pair':pair,'total_senales':len(sigs),'lado_dominante':dom,'nivel_clave':nivel,'vol_total':round(sum(s['volume_eur'] for s in sigs),0),'diff_media':round(sum(s['price_diff_pct'] for s in sigs)/len(sigs),2),'primera_senal':sigs[0]['timestamp'],'ultima_senal':sigs[-1]['timestamp']})
        fields = ['pair','total_senales','lado_dominante','nivel_clave','vol_total','diff_media','primera_senal','ultima_senal']
        filename = 'whale_rafagas.csv'
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
    token = pair.split("/")[0]
    if (token == "EUR"):
        volInEUR = abs(volume)
    elif (token == "ETH2.S"):
        tokenBase = "ETH" + "/EUR"
        pairToNonKraken = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
        volInEUR = abs(volume * eurPrices[pairToNonKraken] * 0.96)
    else:
        tokenBase = token + "/EUR"
        try:
            pairToNonKraken = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
            volInEUR = abs(eurPrices[pairToNonKraken] * volume)
        except:
            try:
                if(token == "USD"):
                    volInEUR = abs(1/eurPrices["ZUSDZEUR"] * volume)
                else:
                    tokenBase = token + "/USD"
                    pairToNonKraken = list(wsnames.keys())[list(wsnames.values()).index(tokenBase)]
                    volInEUR = abs(eurPrices[pairToNonKraken] * volume)
            except:
                try:
                    tokenBase = token + "EUR"
                    volInEUR = abs(eurPrices[tokenBase] * volume)
                except:
                    volInEUR = 0
    return(volInEUR)

def anotateVolume(x):
    x = pd.to_numeric(x)
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

def createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs):
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
    segundaLinea = f"\n*{sign}{round(priceDiff, 2)}%* {changeEmoji} {whaleEmojis}: {volumeBase} {base} ({volumeTokenAnnotated} {token} {volInEUR_annotated}€)"
    firstDate = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[0])).strftime('%Y-%m-%d %H:%M:%S')
    lastDate  = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[-1])).strftime('%S')
    terceraLinea = f"\n{firstDate}::{lastDate}"
    fromPrice = tradeDF["price"].iloc[0]
    toPrice = tradeDF["price"].iloc[-1]
    url = f'[{pair}](https://pro.kraken.com/app/trade/{tokenNormalized}-{baseNormalized})'
    cuartaLinea = f"\n{fromPrice} -> {toPrice} {url}"
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
    return f"{primeraLinea} {segundaLinea} {terceraLinea} {cuartaLinea} {marginMessage}"

def telegram_bot_sendtext(bot_message):
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {'chat_id': bot_chatID, 'parse_mode': "Markdown", 'text': bot_message}
    response = requests.post(url, params=params)
    return response.json()

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
                volInEUR = volumeInEUR(wsnames, pair, volume, local_eurPrices)
                if(volInEUR == 0 or priceDiff > 2 and volInEUR > 15000):
                    priceDiff = round(priceDiff, 3)
                    print(f"\U0001F433 [{label}]", priceDiff, pair)
                    entry_price = float(tradeDF["price"].iloc[-1])
                    signal_id = save_signal(tradeDF, pair, volInEUR, priceDiff)
                    launch_tracker(signal_id, pair, entry_price)
                    TGmsg = createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs)
                    telegram_bot_sendtext(TGmsg)
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
