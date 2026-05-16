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
  :root { --bg:#080c14; --surface:#0d1420; --border:#1a2535; --accent:#00d4ff; --green:#00ff88; --red:#ff4466; --text:#c8d8e8; --muted:#4a6080; }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { background:var(--bg); color:var(--text); font-family:'Space Mono',monospace; min-height:100vh; background-image:radial-gradient(ellipse at 20% 20%,#001830 0%,transparent 60%),radial-gradient(ellipse at 80% 80%,#001020 0%,transparent 60%); }
  header { padding:1.5rem 2rem; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:1rem; }
  header h1 { font-family:'Syne',sans-serif; font-size:1.6rem; font-weight:800; color:var(--accent); }
  header span { color:var(--muted); font-size:0.75rem; }
  .tabs { display:flex; border-bottom:1px solid var(--border); padding:0 2rem; overflow-x:auto; }
  .tab { padding:0.75rem 1.2rem; cursor:pointer; font-size:0.78rem; color:var(--muted); border-bottom:2px solid transparent; transition:all 0.2s; white-space:nowrap; }
  .tab.active { color:var(--accent); border-bottom-color:var(--accent); }
  .tab-content { display:none; }
  .tab-content.active { display:block; }
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:1rem; padding:1.5rem 2rem; border-bottom:1px solid var(--border); }
  .stat { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:1rem; }
  .stat-label { font-size:0.62rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.1em; }
  .stat-value { font-size:1.5rem; font-weight:700; font-family:'Syne',sans-serif; color:var(--accent); margin-top:0.2rem; }
  .top-pairs { padding:1rem 2rem; border-bottom:1px solid var(--border); }
  .top-pairs h3 { font-size:0.65rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.6rem; }
  .pair-bars { display:flex; flex-wrap:wrap; gap:0.4rem; }
  .pair-chip { background:var(--surface); border:1px solid var(--border); border-radius:4px; padding:0.2rem 0.5rem; font-size:0.7rem; cursor:pointer; transition:all 0.15s; }
  .pair-chip:hover { border-color:var(--accent); color:var(--accent); }
  .count { color:var(--muted); margin-left:0.3rem; }
  .controls { padding:1rem 2rem; display:flex; gap:0.75rem; flex-wrap:wrap; align-items:center; border-bottom:1px solid var(--border); }
  input,select { background:var(--surface); border:1px solid var(--border); color:var(--text); padding:0.45rem 0.7rem; border-radius:6px; font-family:'Space Mono',monospace; font-size:0.78rem; outline:none; }
  input:focus,select:focus { border-color:var(--accent); }
  button { background:var(--accent); color:#000; border:none; padding:0.45rem 1rem; border-radius:6px; font-family:'Syne',sans-serif; font-weight:700; font-size:0.78rem; cursor:pointer; transition:opacity 0.2s; }
  button:hover { opacity:0.8; }
  .btn-ghost { background:transparent; border:1px solid var(--border); color:var(--muted); font-size:0.7rem; }
  .btn-ghost:hover { border-color:var(--accent); color:var(--accent); opacity:1; }
  .btn-export { background:#1a3050; color:var(--accent); border:1px solid var(--accent); }
  .btn-export:hover { background:var(--accent); color:#000; opacity:1; }
  .table-wrap { overflow-x:auto; padding-bottom:2rem; }
  table { width:100%; border-collapse:collapse; font-size:0.76rem; }
  th { text-align:left; padding:0.55rem 1rem; color:var(--muted); font-size:0.62rem; text-transform:uppercase; letter-spacing:0.08em; border-bottom:1px solid var(--border); position:sticky; top:0; background:var(--bg); }
  td { padding:0.5rem 1rem; border-bottom:1px solid #0f1a28; }
  tr:hover td { background:var(--surface); }
  .buy { color:var(--green); font-weight:700; }
  .sell { color:var(--red); font-weight:700; }
  .pair-tag { background:#0d1e30; border:1px solid #1a3050; border-radius:4px; padding:0.12rem 0.35rem; font-size:0.68rem; color:var(--accent); }
  .pos { color:var(--green); }
  .neg { color:var(--red); }
  .vol { color:#88aacc; }
  .btn-chart { background:transparent; border:1px solid var(--muted); color:var(--muted); padding:0.15rem 0.5rem; border-radius:4px; font-size:0.65rem; cursor:pointer; font-family:'Space Mono',monospace; }
  .btn-chart:hover { border-color:var(--accent); color:var(--accent); }
  #count { color:var(--muted); font-size:0.73rem; }
  .modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.85); z-index:1000; align-items:center; justify-content:center; }
  .modal-overlay.open { display:flex; }
  .modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:1.5rem; width:90%; max-width:700px; position:relative; max-height:90vh; overflow-y:auto; }
  .modal h2 { font-family:'Syne',sans-serif; font-size:1.1rem; color:var(--accent); margin-bottom:0.3rem; }
  .modal-meta { font-size:0.7rem; color:var(--muted); margin-bottom:1rem; }
  .modal-close { position:absolute; top:1rem; right:1rem; background:transparent; border:1px solid var(--border); color:var(--muted); width:28px; height:28px; border-radius:50%; cursor:pointer; font-size:1rem; display:flex; align-items:center; justify-content:center; }
  .modal-close:hover { border-color:var(--red); color:var(--red); opacity:1; }
  .chart-container { position:relative; height:280px; margin-bottom:1rem; }
  .no-data { text-align:center; color:var(--muted); padding:3rem; font-size:0.8rem; }
  .result-badge { display:inline-block; padding:0.2rem 0.6rem; border-radius:4px; font-size:0.7rem; font-weight:700; }
  .result-pos { background:#00ff8820; color:var(--green); border:1px solid var(--green); }
  .result-neg { background:#ff446620; color:var(--red); border:1px solid var(--red); }
  .results-info { padding:1rem 2rem; color:var(--muted); font-size:0.75rem; border-bottom:1px solid var(--border); }
  .analytics-grid { display:grid; grid-template-columns:1fr 1fr; gap:1.5rem; padding:1.5rem 2rem; }
  @media(max-width:700px){ .analytics-grid { grid-template-columns:1fr; } }
  .analytics-card { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:1.2rem; }
  .analytics-card h3 { font-family:'Syne',sans-serif; font-size:0.85rem; color:var(--accent); margin-bottom:1rem; }
  .analytics-card canvas { max-height:260px; }
  .winrate-row { display:flex; align-items:center; gap:0.5rem; margin-bottom:0.5rem; font-size:0.72rem; }
  .winrate-bar-wrap { flex:1; background:#0f1a28; border-radius:4px; height:8px; overflow:hidden; }
  .winrate-bar { height:100%; border-radius:4px; transition:width 0.5s; }
  .winrate-label { width:80px; color:var(--muted); font-size:0.65rem; }
  .winrate-pct { width:40px; text-align:right; font-weight:700; font-size:0.7rem; }
  .equity-info { font-size:0.7rem; color:var(--muted); margin-bottom:0.75rem; }
  .export-section { padding:1.5rem 2rem; border-top:1px solid var(--border); }
  .export-section h3 { font-family:'Syne',sans-serif; font-size:0.85rem; color:var(--accent); margin-bottom:1rem; }
  .export-grid { display:flex; gap:1rem; flex-wrap:wrap; }
  .export-card { background:var(--surface); border:1px solid var(--border); border-radius:8px; padding:1rem; flex:1; min-width:200px; }
  .export-card p { font-size:0.7rem; color:var(--muted); margin-bottom:0.75rem; line-height:1.5; }
</style>
</head>
<body>
<header>
  <div><h1>🐋 Whale Dashboard</h1><span id="lastUpdate">Cargando...</span></div>
  <button class="btn-ghost" onclick="loadAll()">↻ Actualizar</button>
</header>
<div class="stats" id="stats"></div>
<div class="top-pairs"><h3>Pares más activos — clic para filtrar</h3><div class="pair-bars" id="topPairs"></div></div>
<div class="tabs">
  <div class="tab active" onclick="switchTab('signals')">📋 Señales</div>
  <div class="tab" onclick="switchTab('results')">📊 Resultados 24h</div>
  <div class="tab" onclick="switchTab('analytics')">🔬 Análisis</div>
</div>

<!-- TAB SEÑALES -->
<div class="tab-content active" id="tab-signals">
  <div class="controls">
    <input type="text" id="filterPair" placeholder="Filtrar par (ej: BTC)" onkeyup="if(event.key==='Enter') loadSignals()">
    <select id="filterSide" onchange="loadSignals()">
      <option value="">Todos</option>
      <option value="b">🍏 Buy</option>
      <option value="s">🍎 Sell</option>
    </select>
    <select id="filterLimit" onchange="loadSignals()">
      <option value="100">Últimas 100</option>
      <option value="250">Últimas 250</option>
      <option value="500" selected>Últimas 500</option>
    </select>
    <button onclick="loadSignals()">Buscar</button>
    <span id="count"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>Fecha</th><th>Par</th><th>Lado</th><th>Diff %</th><th>Entrada</th><th>Vol EUR</th><th>Trades</th><th>Chart</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- TAB RESULTADOS -->
<div class="tab-content" id="tab-results">
  <div class="results-info">Señales con tracking completado — % de cambio al final del periodo de seguimiento</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>Fecha</th><th>Par</th><th>Lado</th><th>Diff señal</th><th>Vol EUR</th><th>Resultado</th><th>Horas</th><th>Chart</th></tr></thead>
      <tbody id="tbody-results"></tbody>
    </table>
  </div>
</div>

<!-- TAB ANÁLISIS -->
<div class="tab-content" id="tab-analytics">
  <div class="analytics-grid">
    <!-- Win Rate por par -->
    <div class="analytics-card" style="grid-column:1/-1">
      <h3>🏆 Win Rate por par — % de señales con resultado positivo a 24h</h3>
      <div id="winrate-list"><div class="no-data">Cargando...</div></div>
    </div>
    <!-- Curva de equity -->
    <div class="analytics-card" style="grid-column:1/-1">
      <h3>📈 Curva de equity simulada — resultado acumulado si seguías cada señal</h3>
      <div class="equity-info">Asume entrada al precio de señal, salida a las 24h. Sin comisiones.</div>
      <div class="chart-container"><canvas id="equityChart"></canvas></div>
    </div>
  </div>
  <!-- Exportar CSV -->
  <div class="export-section">
    <h3>📥 Exportar datos</h3>
    <div class="export-grid">
      <div class="export-card">
        <p>Todas las señales con sus datos completos. Ideal para análisis con IA.</p>
        <button class="btn-export" onclick="exportCSV('signals')">⬇ Señales CSV</button>
      </div>
      <div class="export-card">
        <p>Señales con resultado final a 24h. Para analizar qué estrategia funciona.</p>
        <button class="btn-export" onclick="exportCSV('results')">⬇ Resultados CSV</button>
      </div>
      <div class="export-card">
        <p>Win rate por par ordenado. Para saber qué monedas seguir.</p>
        <button class="btn-export" onclick="exportCSV('winrate')">⬇ Win Rate CSV</button>
      </div>
    </div>
  </div>
</div>

<!-- MODAL GRÁFICA -->
<div class="modal-overlay" id="modal">
  <div class="modal">
    <button class="modal-close" onclick="closeModal()">×</button>
    <h2 id="modal-title">—</h2>
    <div class="modal-meta" id="modal-meta"></div>
    <div class="chart-container"><canvas id="priceChart"></canvas></div>
    <div id="no-data" class="no-data" style="display:none">⏳ Sin datos de tracking aún — empieza 5 minutos después de la señal</div>
  </div>
</div>

<script>
let chartInstance = null;
let equityChart = null;

function fmt(n) {
  if (!n && n!==0) return '—';
  if (Math.abs(n)>=1e6) return (n/1e6).toFixed(2)+'M';
  if (Math.abs(n)>=1e3) return (n/1e3).toFixed(2)+'K';
  return parseFloat(n).toFixed(2);
}

function switchTab(tab) {
  const tabs = ['signals','results','analytics'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active', tabs[i]===tab));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  if(tab==='results') loadResults();
  if(tab==='analytics') loadAnalytics();
}

async function loadAll() {
  await loadStats();
  await loadSignals();
  document.getElementById('lastUpdate').textContent='Actualizado: '+new Date().toLocaleTimeString();
}

async function loadStats() {
  const s=await(await fetch('/api/stats')).json();
  document.getElementById('stats').innerHTML=`
    <div class="stat"><div class="stat-label">Total señales</div><div class="stat-value">${s.total}</div></div>
    <div class="stat"><div class="stat-label">Compras 🍏</div><div class="stat-value" style="color:var(--green)">${s.buys}</div></div>
    <div class="stat"><div class="stat-label">Ventas 🍎</div><div class="stat-value" style="color:var(--red)">${s.sells}</div></div>
    <div class="stat"><div class="stat-label">Con tracking</div><div class="stat-value" style="color:#ffaa00">${s.tracked}</div></div>
    <div class="stat"><div class="stat-label">Diff media</div><div class="stat-value">${s.avg_diff}%</div></div>
    <div class="stat"><div class="stat-label">Vol medio €</div><div class="stat-value">${fmt(s.avg_vol)}</div></div>`;
  document.getElementById('topPairs').innerHTML=s.top_pairs.map(p=>
    `<div class="pair-chip" onclick="filterByPair('${p.pair}')">${p.pair}<span class="count">${p.count}</span></div>`).join('');
}

function filterByPair(pair) {
  document.getElementById('filterPair').value=pair.replace('/','');
  switchTab('signals'); loadSignals();
}

async function loadSignals() {
  const pair=document.getElementById('filterPair').value;
  const side=document.getElementById('filterSide').value;
  const limit=document.getElementById('filterLimit').value;
  const data=await(await fetch(`/api/signals?pair=${pair}&side=${side}&limit=${limit}`)).json();
  document.getElementById('count').textContent=`${data.length} señales`;
  document.getElementById('tbody').innerHTML=data.map(s=>{
    const sideHtml=s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>';
    const diff=s.price_diff_pct>=0?`<span class="pos">+${s.price_diff_pct}%</span>`:`<span class="neg">${s.price_diff_pct}%</span>`;
    return `<tr><td style="color:var(--muted)">#${s.id}</td><td style="color:var(--muted);font-size:0.7rem">${s.timestamp.replace('T',' ').substring(0,19)}</td><td><span class="pair-tag">${s.pair}</span></td><td>${sideHtml}</td><td>${diff}</td><td class="vol" style="font-size:0.7rem">${parseFloat(s.price_to).toPrecision(4)}</td><td class="vol">${fmt(s.volume_eur)}€</td><td style="color:var(--muted)">${s.num_trades}</td><td><button class="btn-chart" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_to})">📈</button></td></tr>`;
  }).join('')||'<tr><td colspan="9" style="text-align:center;color:var(--muted);padding:2rem">No hay señales</td></tr>';
}

async function loadResults() {
  const data=await(await fetch('/api/results')).json();
  document.getElementById('tbody-results').innerHTML=data.map(s=>{
    const sideHtml=s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>';
    const diff=s.price_diff_pct>=0?`<span class="pos">+${s.price_diff_pct}%</span>`:`<span class="neg">${s.price_diff_pct}%</span>`;
    const result=s.final_change>=0?`<span class="result-badge result-pos">+${s.final_change}%</span>`:`<span class="result-badge result-neg">${s.final_change}%</span>`;
    return `<tr><td style="color:var(--muted)">#${s.id}</td><td style="color:var(--muted);font-size:0.7rem">${s.timestamp.substring(0,16)}</td><td><span class="pair-tag">${s.pair}</span></td><td>${sideHtml}</td><td>${diff}</td><td class="vol">${fmt(s.volume_eur)}€</td><td>${result}</td><td style="color:var(--muted)">${Math.round(s.final_minutes/60)}h</td><td><button class="btn-chart" onclick="openChart(${s.id},'${s.pair}','${s.side}',null)">📈</button></td></tr>`;
  }).join('')||'<tr><td colspan="9" style="text-align:center;color:var(--muted);padding:2rem">Aún no hay señales con tracking completado</td></tr>';
}

async function loadAnalytics() {
  const [wr, eq] = await Promise.all([
    fetch('/api/winrate').then(r=>r.json()),
    fetch('/api/equity').then(r=>r.json())
  ]);
  renderWinRate(wr);
  renderEquity(eq);
}

function renderWinRate(data) {
  if (!data || data.length===0) {
    document.getElementById('winrate-list').innerHTML='<div class="no-data">Sin datos de tracking aún — necesitas señales con 24h completadas</div>';
    return;
  }
  const maxCount = Math.max(...data.map(d=>d.total));
  document.getElementById('winrate-list').innerHTML = data.map(d => {
    const pct = d.winrate;
    const color = pct>=60?'#00ff88':pct>=40?'#ffaa00':'#ff4466';
    const sideLabel = d.side==='b'?'🍏':(d.side==='s'?'🍎':'📊');
    return `<div class="winrate-row">
      <div class="winrate-label">${sideLabel} <span style="color:var(--accent)">${d.pair}</span></div>
      <div class="winrate-bar-wrap"><div class="winrate-bar" style="width:${pct}%;background:${color}"></div></div>
      <div class="winrate-pct" style="color:${color}">${pct}%</div>
      <div style="color:var(--muted);font-size:0.62rem;width:50px">${d.wins}/${d.total}</div>
    </div>`;
  }).join('');
}

function renderEquity(data) {
  if (!data || data.length===0) {
    return;
  }
  if (equityChart) { equityChart.destroy(); equityChart=null; }
  const labels = data.map(d=>d.timestamp.substring(0,10));
  const values = data.map(d=>d.cumulative);
  const lastVal = values[values.length-1] || 0;
  const color = lastVal>=0?'#00ff88':'#ff4466';
  equityChart = new Chart(document.getElementById('equityChart'), {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Equity acumulado %',
        data: values,
        borderColor: color,
        backgroundColor: color+'15',
        borderWidth: 2,
        pointRadius: 2,
        tension: 0.3,
        fill: true
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color:'#4a6080', font:{family:'Space Mono',size:10} } },
        tooltip: { backgroundColor:'#0d1420', borderColor:'#1a2535', borderWidth:1, titleColor:'#c8d8e8', bodyColor:'#c8d8e8' }
      },
      scales: {
        x: { ticks:{color:'#4a6080',font:{size:9},maxTicksLimit:10}, grid:{color:'#0f1a28'} },
        y: { ticks:{color:color,font:{size:9},callback:v=>v+'%'}, grid:{color:'#0f1a28'} }
      }
    }
  });
}

async function openChart(signalId,pair,side,entryPrice) {
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

function exportCSV(type) {
  window.location.href='/api/export/'+type;
}

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
        'top_pairs': db_get("SELECT pair, COUNT(*) as count FROM signals GROUP BY pair ORDER BY count DESC LIMIT 10")
    })

@app.route('/api/tracking/<int:signal_id>')
def api_tracking(signal_id):
    rows   = db_get("SELECT minutes,price,pct_change,timestamp FROM price_tracking WHERE signal_id=? ORDER BY minutes", [signal_id])
    signal = db_get("SELECT * FROM signals WHERE id=?", [signal_id])
    return jsonify({"signal": signal[0] if signal else {}, "tracking": rows})

@app.route('/api/results')
def api_results():
    return jsonify(db_get("""
        SELECT s.id,s.pair,s.side,s.timestamp,s.price_diff_pct,s.volume_eur,
               pt.pct_change as final_change, pt.minutes as final_minutes
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
        ORDER BY s.id DESC LIMIT 200
    """))

@app.route('/api/winrate')
def api_winrate():
    # Win rate por par y lado: % de señales con resultado final positivo
    data = db_get("""
        SELECT s.pair, s.side,
               COUNT(*) as total,
               SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) as wins,
               ROUND(100.0 * SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as winrate
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
        GROUP BY s.pair, s.side
        HAVING total >= 2
        ORDER BY winrate DESC, total DESC
        LIMIT 30
    """)
    return jsonify(data)

@app.route('/api/equity')
def api_equity():
    # Curva de equity: suma acumulada de pct_change de cada señal finalizada
    data = db_get("""
        SELECT s.timestamp, s.pair, s.side, pt.pct_change
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
        ORDER BY s.timestamp ASC
    """)
    cumulative = 0.0
    result = []
    for row in data:
        cumulative = round(cumulative + (row['pct_change'] or 0), 2)
        result.append({'timestamp': row['timestamp'], 'pair': row['pair'], 'pct_change': row['pct_change'], 'cumulative': cumulative})
    return jsonify(result)

@app.route('/api/export/<export_type>')
def api_export(export_type):
    from flask import Response
    import io, csv
    if export_type == 'signals':
        data = db_get("SELECT * FROM signals ORDER BY id DESC")
        fields = ['id','timestamp','pair','side','price_from','price_to','price_diff_pct','volume_token','volume_eur','order_type','num_trades']
        filename = 'whale_signals.csv'
    elif export_type == 'results':
        data = db_get("""
            SELECT s.id,s.timestamp,s.pair,s.side,s.price_diff_pct,s.volume_eur,
                   pt.pct_change as resultado_24h, pt.minutes as minutos_tracking
            FROM signals s JOIN price_tracking pt ON pt.signal_id=s.id
            WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
            ORDER BY s.id DESC
        """)
        fields = ['id','timestamp','pair','side','price_diff_pct','volume_eur','resultado_24h','minutos_tracking']
        filename = 'whale_results.csv'
    elif export_type == 'winrate':
        data = db_get("""
            SELECT s.pair, s.side, COUNT(*) as total,
                   SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) as wins,
                   ROUND(100.0 * SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as winrate
            FROM signals s JOIN price_tracking pt ON pt.signal_id=s.id
            WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
            GROUP BY s.pair, s.side HAVING total >= 2 ORDER BY winrate DESC
        """)
        fields = ['pair','side','total','wins','winrate']
        filename = 'whale_winrate.csv'
    else:
        return jsonify({'error': 'Unknown export type'})
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(data)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment;filename={filename}'})
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
                if(volInEUR == 0 or priceDiff > 1 and volInEUR > 1000):
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
