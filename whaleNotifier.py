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
:root{--bg:#080c14;--surface:#0d1420;--border:#1a2535;--accent:#00d4ff;--green:#00ff88;--red:#ff4466;--orange:#ffaa00;--text:#c8d8e8;--muted:#4a6080}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Space Mono',monospace;min-height:100vh;background-image:radial-gradient(ellipse at 20% 20%,#001830 0%,transparent 60%),radial-gradient(ellipse at 80% 80%,#001020 0%,transparent 60%)}
header{padding:1.2rem 2rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:1rem;flex-wrap:wrap}
header h1{font-family:'Syne',sans-serif;font-size:1.5rem;font-weight:800;color:var(--accent)}
header span{color:var(--muted);font-size:0.72rem}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:0.75rem;padding:1.2rem 2rem;border-bottom:1px solid var(--border)}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:0.9rem}
.stat-label{font-size:0.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.1em}
.stat-value{font-size:1.4rem;font-weight:700;font-family:'Syne',sans-serif;color:var(--accent);margin-top:0.15rem}
.tabs{display:flex;border-bottom:1px solid var(--border);padding:0 2rem;overflow-x:auto}
.tab{padding:0.7rem 1.2rem;cursor:pointer;font-size:0.75rem;color:var(--muted);border-bottom:2px solid transparent;transition:all 0.2s;white-space:nowrap}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab-content{display:none}
.tab-content.active{display:block}
.section-pad{padding:1.5rem 2rem}
.section-title{font-family:'Syne',sans-serif;font-size:0.9rem;font-weight:700;color:var(--accent);margin-bottom:0.4rem}
.section-sub{font-size:0.68rem;color:var(--muted);margin-bottom:1.2rem}
input,select{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:0.4rem 0.65rem;border-radius:6px;font-family:'Space Mono',monospace;font-size:0.75rem;outline:none}
input:focus,select:focus{border-color:var(--accent)}
button{background:var(--accent);color:#000;border:none;padding:0.4rem 0.9rem;border-radius:6px;font-family:'Syne',sans-serif;font-weight:700;font-size:0.75rem;cursor:pointer;transition:opacity 0.2s}
button:hover{opacity:0.8}
.btn-ghost{background:transparent;border:1px solid var(--border);color:var(--muted);font-size:0.68rem}
.btn-ghost:hover{border-color:var(--accent);color:var(--accent);opacity:1}
.btn-export{background:#1a3050;color:var(--accent);border:1px solid #1a4060}
.btn-export:hover{background:var(--accent);color:#000;opacity:1}
.controls{padding:0.9rem 2rem;display:flex;gap:0.6rem;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--border)}
.table-wrap{overflow-x:auto;padding-bottom:2rem}
table{width:100%;border-collapse:collapse;font-size:0.74rem}
th{text-align:left;padding:0.5rem 1rem;color:var(--muted);font-size:0.6rem;text-transform:uppercase;letter-spacing:0.08em;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg)}
td{padding:0.45rem 1rem;border-bottom:1px solid #0f1a28}
tr:hover td{background:var(--surface)}
.buy{color:var(--green);font-weight:700}
.sell{color:var(--red);font-weight:700}
.pair-tag{background:#0d1e30;border:1px solid #1a3050;border-radius:4px;padding:0.1rem 0.35rem;font-size:0.66rem;color:var(--accent);cursor:pointer}
.pair-tag:hover{border-color:var(--accent)}
.pos{color:var(--green)}
.neg{color:var(--red)}
.neu{color:var(--orange)}
.vol{color:#88aacc}
.muted{color:var(--muted)}
.btn-sm{background:transparent;border:1px solid var(--muted);color:var(--muted);padding:0.12rem 0.45rem;border-radius:4px;font-size:0.62rem;cursor:pointer;font-family:'Space Mono',monospace}
.btn-sm:hover{border-color:var(--accent);color:var(--accent)}
#count{color:var(--muted);font-size:0.72rem}
/* CANDIDATOS */
.candidates-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:1rem;padding:1.5rem 2rem}
.candidate-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1.2rem;cursor:pointer;transition:border-color 0.2s}
.candidate-card:hover{border-color:var(--accent)}
.cand-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:0.9rem}
.cand-pair{font-family:'Syne',sans-serif;font-size:1rem;font-weight:700;color:var(--accent)}
.score-badge{font-family:'Syne',sans-serif;font-size:0.8rem;font-weight:800;padding:0.2rem 0.6rem;border-radius:20px}
.score-high{background:#00ff8820;color:var(--green);border:1px solid var(--green)}
.score-med{background:#ffaa0020;color:var(--orange);border:1px solid var(--orange)}
.score-low{background:#ff446620;color:var(--red);border:1px solid var(--red)}
.cand-metrics{display:grid;grid-template-columns:1fr 1fr;gap:0.5rem}
.cand-metric{background:#0a1220;border-radius:6px;padding:0.5rem 0.7rem}
.cand-metric-label{font-size:0.58rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.08em}
.cand-metric-value{font-size:0.9rem;font-weight:700;font-family:'Syne',sans-serif;margin-top:0.1rem}
.cand-order{margin-top:0.9rem;background:#0a1830;border:1px solid #1a3050;border-radius:6px;padding:0.6rem 0.8rem;font-size:0.68rem}
.cand-order-label{color:var(--muted);font-size:0.58rem;text-transform:uppercase;margin-bottom:0.2rem}
.cand-order-price{color:var(--green);font-weight:700;font-size:0.85rem}
/* MECHAZOS */
.mecho-row{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 1fr 80px;gap:0;align-items:center}
/* MODAL */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,0.88);z-index:1000;align-items:center;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.5rem;width:92%;max-width:720px;position:relative;max-height:90vh;overflow-y:auto}
.modal h2{font-family:'Syne',sans-serif;font-size:1.1rem;color:var(--accent);margin-bottom:0.25rem}
.modal-meta{font-size:0.68rem;color:var(--muted);margin-bottom:1rem}
.modal-close{position:absolute;top:1rem;right:1rem;background:transparent;border:1px solid var(--border);color:var(--muted);width:28px;height:28px;border-radius:50%;cursor:pointer;font-size:1rem;display:flex;align-items:center;justify-content:center}
.modal-close:hover{border-color:var(--red);color:var(--red);opacity:1}
.chart-container{position:relative;height:260px;margin-bottom:1rem}
.no-data{text-align:center;color:var(--muted);padding:2.5rem;font-size:0.78rem}
.result-badge{display:inline-block;padding:0.18rem 0.55rem;border-radius:4px;font-size:0.68rem;font-weight:700}
.result-pos{background:#00ff8820;color:var(--green);border:1px solid var(--green)}
.result-neg{background:#ff446620;color:var(--red);border:1px solid var(--red)}
/* REVERTION BARS */
.rev-grid{display:grid;grid-template-columns:60px 1fr 50px;gap:0.4rem;align-items:center;margin-bottom:0.4rem;font-size:0.68rem}
.rev-bar-wrap{background:#0f1a28;border-radius:4px;height:7px;overflow:hidden}
.rev-bar{height:100%;border-radius:4px}
/* EXPORT */
.export-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem}
.export-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem}
.export-card p{font-size:0.68rem;color:var(--muted);margin-bottom:0.75rem;line-height:1.5}
.pair-chips{display:flex;flex-wrap:wrap;gap:0.35rem;padding:0.75rem 2rem;border-bottom:1px solid var(--border)}
.chip{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:0.18rem 0.5rem;font-size:0.68rem;cursor:pointer;transition:all 0.15s}
.chip:hover{border-color:var(--accent);color:var(--accent)}
.chip .cnt{color:var(--muted);margin-left:0.25rem}
</style>
</head>
<body>
<header>
  <div><h1>🐋 Whale Dashboard</h1><span id="lastUpdate">Cargando...</span></div>
  <button class="btn-ghost" onclick="loadAll()">↻ Actualizar</button>
</header>
<div class="stats" id="stats"></div>
<div class="pair-chips" id="topPairs"></div>

<div class="tabs">
  <div class="tab active" onclick="switchTab('candidates')">🎯 Candidatos</div>
  <div class="tab" onclick="switchTab('mechazos')">⚡ Mechazos</div>
  <div class="tab" onclick="switchTab('par')">🔍 Por par</div>
  <div class="tab" onclick="switchTab('signals')">📋 Señales</div>
  <div class="tab" onclick="switchTab('export')">📥 Exportar</div>
</div>

<!-- TAB CANDIDATOS -->
<div class="tab-content active" id="tab-candidates">
  <div class="section-pad">
    <div class="section-title">🎯 Pares candidatos para cazar mechazos</div>
    <div class="section-sub">Ordenados por score: % reversión × frecuencia × tamaño. Verde &gt;7, naranja &gt;4, rojo &lt;4. El precio de orden es el extremo medio del mecho.</div>
  </div>
  <div class="candidates-grid" id="candidates-grid">
    <div class="no-data">Cargando candidatos...</div>
  </div>
</div>

<!-- TAB MECHAZOS -->
<div class="tab-content" id="tab-mechazos">
  <div class="section-pad" style="padding-bottom:0.5rem">
    <div class="section-title">⚡ Mechazos recientes</div>
    <div class="section-sub">Cada señal de ballena con su % de reversión actual. Si la columna "Rev. ahora" es alta → el precio volvió. Si es baja → el mecho sigue abierto.</div>
  </div>
  <div class="controls">
    <input type="text" id="mechazo-pair" placeholder="Filtrar par" onkeyup="if(event.key==='Enter') loadMechazos()">
    <select id="mechazo-side" onchange="loadMechazos()">
      <option value="">Todos</option>
      <option value="b">🍏 Compra ballena</option>
      <option value="s">🍎 Venta ballena</option>
    </select>
    <button onclick="loadMechazos()">Filtrar</button>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>#</th><th>Fecha</th><th>Par</th><th>Lado ballena</th>
        <th>Diff %</th><th>Precio extremo</th><th>Vol €</th>
        <th>Rev. 1h</th><th>Rev. 4h</th><th>Rev. 24h</th><th>Chart</th>
      </tr></thead>
      <tbody id="tbody-mechazos"></tbody>
    </table>
  </div>
</div>

<!-- TAB POR PAR -->
<div class="tab-content" id="tab-par">
  <div class="section-pad" style="padding-bottom:0">
    <div class="section-title">🔍 Análisis por par</div>
    <div class="section-sub">Selecciona un par para ver su historial de mechazos, % reversión por intervalo y volumen mínimo rentable.</div>
    <div style="display:flex;gap:0.75rem;align-items:center;margin-bottom:1rem">
      <input type="text" id="par-input" placeholder="Ej: BTC/EUR" style="width:160px">
      <button onclick="loadPar()">Analizar</button>
    </div>
  </div>
  <div id="par-content" class="section-pad" style="padding-top:0"></div>
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
    <span id="count"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>Fecha</th><th>Par</th><th>Lado</th><th>Diff %</th><th>Precio entrada</th><th>Vol EUR</th><th>Trades</th><th>Chart</th></tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- TAB EXPORTAR -->
<div class="tab-content" id="tab-export">
  <div class="section-pad">
    <div class="section-title">📥 Exportar datos para análisis IA</div>
    <div class="section-sub">Descarga los datos en CSV para analizar con Claude u otras herramientas.</div>
    <div class="export-grid">
      <div class="export-card">
        <div class="section-title" style="font-size:0.78rem;margin-bottom:0.5rem">Todas las señales</div>
        <p>Cada señal de ballena con par, lado, precio, volumen y timestamp. Base para cualquier análisis.</p>
        <button class="btn-export" onclick="exportCSV('signals')">⬇ Descargar</button>
      </div>
      <div class="export-card">
        <div class="section-title" style="font-size:0.78rem;margin-bottom:0.5rem">Resultados 24h</div>
        <p>Señales con su resultado final. Para analizar qué señales fueron rentables y cuáles no.</p>
        <button class="btn-export" onclick="exportCSV('results')">⬇ Descargar</button>
      </div>
      <div class="export-card">
        <div class="section-title" style="font-size:0.78rem;margin-bottom:0.5rem">Análisis mechazos</div>
        <p>Par, frecuencia, % reversión media a 1h/4h/24h, vol mínimo. Ideal para definir estrategia.</p>
        <button class="btn-export" onclick="exportCSV('mechazos')">⬇ Descargar</button>
      </div>
      <div class="export-card">
        <div class="section-title" style="font-size:0.78rem;margin-bottom:0.5rem">Win Rate por par</div>
        <p>Ranking de pares por % de señales con resultado positivo a 24h.</p>
        <button class="btn-export" onclick="exportCSV('winrate')">⬇ Descargar</button>
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
  return `<span class="${c}">${v>0?'+':''}${v}%</span>`;
}
function scoreClass(s){ return s>=7?'score-high':s>=4?'score-med':'score-low'; }

function switchTab(tab){
  const tabs=['candidates','mechazos','par','signals','export'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===tab));
  document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));
  document.getElementById('tab-'+tab).classList.add('active');
  if(tab==='candidates') loadCandidates();
  if(tab==='mechazos') loadMechazos();
  if(tab==='signals') loadSignals();
}

async function loadAll(){
  await Promise.all([loadStats(), loadCandidates()]);
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

async function loadCandidates(){
  const data=await(await fetch('/api/candidates')).json();
  if(!data||data.length===0){
    document.getElementById('candidates-grid').innerHTML='<div class="no-data" style="grid-column:1/-1">Necesitas señales con tracking completado (24h) para ver candidatos.<br><br>Mientras tanto, revisa la pestaña ⚡ Mechazos para ver los mechazos en tiempo real.</div>';
    return;
  }
  document.getElementById('candidates-grid').innerHTML=data.map(d=>{
    const sc=parseFloat(d.score);
    const scClass=scoreClass(sc);
    const sideLabel=d.side==='b'?'🍏 Compras ballena':'🍎 Ventas ballena';
    const orderSide=d.side==='b'?'COMPRA límite':'VENTA límite';
    const orderColor=d.side==='b'?'var(--green)':'var(--red)';
    return `<div class="candidate-card" onclick="goToPar('${d.pair}')">
      <div class="cand-header">
        <div>
          <div class="cand-pair">${d.pair}</div>
          <div style="font-size:0.62rem;color:var(--muted);margin-top:0.1rem">${sideLabel}</div>
        </div>
        <div class="score-badge ${scClass}">Score ${sc}</div>
      </div>
      <div class="cand-metrics">
        <div class="cand-metric">
          <div class="cand-metric-label">% Reversión media</div>
          <div class="cand-metric-value ${d.avg_reversion>50?'pos':'neg'}">${d.avg_reversion}%</div>
        </div>
        <div class="cand-metric">
          <div class="cand-metric-label">Frecuencia</div>
          <div class="cand-metric-value" style="color:var(--accent)">${d.frequency} señales</div>
        </div>
        <div class="cand-metric">
          <div class="cand-metric-label">Vol mínimo</div>
          <div class="cand-metric-value vol">${fmt(d.min_vol)}€</div>
        </div>
        <div class="cand-metric">
          <div class="cand-metric-label">Diff media señal</div>
          <div class="cand-metric-value neu">${d.avg_diff}%</div>
        </div>
      </div>
      <div class="cand-order">
        <div class="cand-order-label">💡 Orden sugerida — ${orderSide}</div>
        <div class="cand-order-price" style="color:${orderColor}">En nivel extremo del mecho · Ver pestaña 🔍 Por par</div>
      </div>
    </div>`;
  }).join('');
}

async function loadMechazos(){
  const pair=document.getElementById('mechazo-pair').value;
  const side=document.getElementById('mechazo-side').value;
  const data=await(await fetch(`/api/mechazos?pair=${pair}&side=${side}`)).json();
  document.getElementById('tbody-mechazos').innerHTML=data.map(s=>{
    const sideHtml=s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>';
    const rev1=s.rev_1h!==null?pct(s.rev_1h):'<span class="muted">—</span>';
    const rev4=s.rev_4h!==null?pct(s.rev_4h):'<span class="muted">—</span>';
    const rev24=s.rev_24h!==null?pct(s.rev_24h):'<span class="muted">—</span>';
    return `<tr>
      <td class="muted">#${s.id}</td>
      <td class="muted" style="font-size:0.68rem">${s.timestamp.replace('T',' ').substring(0,16)}</td>
      <td><span class="pair-tag" onclick="goToPar('${s.pair}')">${s.pair}</span></td>
      <td>${sideHtml}</td>
      <td>${pct(s.price_diff_pct)}</td>
      <td class="vol" style="font-size:0.7rem">${parseFloat(s.price_extremo).toPrecision(5)}</td>
      <td class="vol">${fmt(s.volume_eur)}€</td>
      <td>${rev1}</td>
      <td>${rev4}</td>
      <td>${rev24}</td>
      <td><button class="btn-sm" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_extremo})">📈</button></td>
    </tr>`;
  }).join('')||'<tr><td colspan="11" class="no-data">No hay mechazos</td></tr>';
}

async function loadPar(){
  const pair=document.getElementById('par-input').value.trim();
  if(!pair)return;
  const data=await(await fetch('/api/par?pair='+encodeURIComponent(pair))).json();
  const el=document.getElementById('par-content');
  if(!data||data.total===0){
    el.innerHTML='<div class="no-data">No hay datos para '+pair+'</div>';
    return;
  }
  const rev1c=data.rev_1h_media>50?'pos':'neg';
  const rev4c=data.rev_4h_media>50?'pos':'neg';
  const rev24c=data.rev_24h_media>50?'pos':'neg';
  el.innerHTML=`
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:0.75rem;margin-bottom:1.5rem">
      <div class="cand-metric"><div class="cand-metric-label">Total mechazos</div><div class="cand-metric-value" style="color:var(--accent)">${data.total}</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 1h</div><div class="cand-metric-value ${rev1c}">${data.rev_1h_media}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 4h</div><div class="cand-metric-value ${rev4c}">${data.rev_4h_media}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Rev. media 24h</div><div class="cand-metric-value ${rev24c}">${data.rev_24h_media}%</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol mínimo señal</div><div class="cand-metric-value vol">${fmt(data.vol_min)}€</div></div>
      <div class="cand-metric"><div class="cand-metric-label">Vol medio señal</div><div class="cand-metric-value vol">${fmt(data.vol_avg)}€</div></div>
    </div>
    <div style="font-size:0.7rem;color:var(--muted);margin-bottom:0.75rem">REVERSIÓN POR INTERVALO (% del mecho revertido de media)</div>
    <div style="max-width:400px;margin-bottom:1.5rem">
      <div class="rev-grid"><span class="muted">1 hora</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_1h_media,100)}%;background:${data.rev_1h_media>50?'var(--green)':'var(--red)'}"></div></div><span class="${rev1c}">${data.rev_1h_media}%</span></div>
      <div class="rev-grid"><span class="muted">4 horas</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_4h_media,100)}%;background:${data.rev_4h_media>50?'var(--green)':'var(--red)'}"></div></div><span class="${rev4c}">${data.rev_4h_media}%</span></div>
      <div class="rev-grid"><span class="muted">24 horas</span><div class="rev-bar-wrap"><div class="rev-bar" style="width:${Math.min(data.rev_24h_media,100)}%;background:${data.rev_24h_media>50?'var(--green)':'var(--red)'}"></div></div><span class="${rev24c}">${data.rev_24h_media}%</span></div>
    </div>
    <div style="font-size:0.7rem;color:var(--muted);margin-bottom:0.75rem">MECHAZOS HISTÓRICOS</div>
    <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>Fecha</th><th>Lado</th><th>Diff %</th><th>Precio extremo</th><th>Vol €</th><th>Rev 1h</th><th>Rev 4h</th><th>Rev 24h</th><th>Chart</th></tr></thead>
      <tbody>${(data.signals||[]).map(s=>`<tr>
        <td class="muted">#${s.id}</td>
        <td class="muted" style="font-size:0.68rem">${s.timestamp.replace('T',' ').substring(0,16)}</td>
        <td>${s.side==='b'?'<span class="buy">🍏</span>':'<span class="sell">🍎</span>'}</td>
        <td>${pct(s.price_diff_pct)}</td>
        <td class="vol">${parseFloat(s.price_extremo).toPrecision(5)}</td>
        <td class="vol">${fmt(s.volume_eur)}€</td>
        <td>${pct(s.rev_1h)}</td>
        <td>${pct(s.rev_4h)}</td>
        <td>${pct(s.rev_24h)}</td>
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
  document.getElementById('tbody').innerHTML=data.map(s=>{
    const sideHtml=s.side==='b'?'<span class="buy">🍏 BUY</span>':'<span class="sell">🍎 SELL</span>';
    const diff=s.price_diff_pct>=0?`<span class="pos">+${s.price_diff_pct}%</span>`:`<span class="neg">${s.price_diff_pct}%</span>`;
    return `<tr>
      <td class="muted">#${s.id}</td>
      <td class="muted" style="font-size:0.68rem">${s.timestamp.replace('T',' ').substring(0,19)}</td>
      <td><span class="pair-tag" onclick="goToPar('${s.pair}')">${s.pair}</span></td>
      <td>${sideHtml}</td><td>${diff}</td>
      <td class="vol" style="font-size:0.7rem">${parseFloat(s.price_to).toPrecision(5)}</td>
      <td class="vol">${fmt(s.volume_eur)}€</td>
      <td class="muted">${s.num_trades}</td>
      <td><button class="btn-sm" onclick="openChart(${s.id},'${s.pair}','${s.side}',${s.price_to})">📈</button></td>
    </tr>`;
  }).join('')||'<tr><td colspan="9" class="no-data">No hay señales</td></tr>';
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
setInterval(loadAll,60000);
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
    """Obtiene el % de cambio más cercano al minuto objetivo."""
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
        # El precio extremo es price_from para ventas de ballena, price_to para compras
        # En una venta masiva el extremo bajo es price_to, en compra masiva el extremo alto es price_to
        price_extremo = s['price_to']
        rev_1h  = get_reversion(s['id'], 60)
        rev_4h  = get_reversion(s['id'], 240)
        rev_24h = get_reversion(s['id'], 1440)
        result.append({**s, 'price_extremo': price_extremo, 'rev_1h': rev_1h, 'rev_4h': rev_4h, 'rev_24h': rev_24h})
    return jsonify(result)

@app.route('/api/candidates')
def api_candidates():
    # Pares con suficientes señales con tracking para calcular reversión
    pairs_data = db_get("""
        SELECT DISTINCT s.pair, s.side
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        GROUP BY s.pair, s.side
        HAVING COUNT(DISTINCT s.id) >= 2
    """)
    result = []
    for pd_row in pairs_data:
        pair = pd_row['pair']
        side = pd_row['side']
        signals = db_get("SELECT * FROM signals WHERE pair=? AND side=? ORDER BY id DESC", [pair, side])
        revs_24h = []
        vols = []
        diffs = []
        for s in signals:
            r = get_reversion(s['id'], 1440)
            if r is not None:
                revs_24h.append(r)
            vols.append(s['volume_eur'])
            diffs.append(s['price_diff_pct'])
        if not revs_24h:
            continue
        avg_rev = round(sum(revs_24h) / len(revs_24h), 1)
        # Para cazador de mechazos: reversión positiva significa precio volvió
        # Si ballena vende (s) y precio cae, reversión positiva = precio subió de vuelta
        # Si ballena compra (b) y precio sube, reversión... depende del contexto
        avg_reversion_pct = round(abs(avg_rev), 1)
        frequency = len(signals)
        min_vol = round(min(vols), 0) if vols else 0
        avg_diff = round(sum(diffs)/len(diffs), 2) if diffs else 0
        # Score: reversión × log(frecuencia) × (diff media / 2)
        import math as _math
        score = round(avg_reversion_pct/10 * _math.log(frequency+1) * (avg_diff/2 + 1), 1)
        result.append({
            'pair': pair, 'side': side,
            'avg_reversion': avg_reversion_pct,
            'frequency': frequency,
            'min_vol': min_vol,
            'avg_diff': avg_diff,
            'score': score
        })
    result.sort(key=lambda x: x['score'], reverse=True)
    return jsonify(result[:20])

@app.route('/api/par')
def api_par():
    pair = flask_request.args.get('pair', '')
    if not pair:
        return jsonify({})
    signals = db_get("SELECT * FROM signals WHERE pair LIKE ? ORDER BY id DESC", [f"%{pair}%"])
    if not signals:
        return jsonify({'total': 0})
    vols = [s['volume_eur'] for s in signals]
    diffs = [s['price_diff_pct'] for s in signals]
    revs_1h, revs_4h, revs_24h = [], [], []
    enriched = []
    for s in signals:
        r1  = get_reversion(s['id'], 60)
        r4  = get_reversion(s['id'], 240)
        r24 = get_reversion(s['id'], 1440)
        if r1  is not None: revs_1h.append(abs(r1))
        if r4  is not None: revs_4h.append(abs(r4))
        if r24 is not None: revs_24h.append(abs(r24))
        enriched.append({**s, 'price_extremo': s['price_to'], 'rev_1h': r1, 'rev_4h': r4, 'rev_24h': r24})
    avg = lambda lst: round(sum(lst)/len(lst), 1) if lst else None
    return jsonify({
        'total': len(signals),
        'rev_1h_media':  avg(revs_1h),
        'rev_4h_media':  avg(revs_4h),
        'rev_24h_media': avg(revs_24h),
        'vol_min': round(min(vols), 0),
        'vol_avg': round(sum(vols)/len(vols), 0),
        'avg_diff': round(sum(diffs)/len(diffs), 2),
        'signals': enriched[:50]
    })

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
    data = db_get("""
        SELECT s.pair, s.side,
               COUNT(*) as total,
               SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) as wins,
               ROUND(100.0 * SUM(CASE WHEN pt.pct_change > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as winrate
        FROM signals s
        JOIN price_tracking pt ON pt.signal_id=s.id
        WHERE pt.minutes=(SELECT MAX(minutes) FROM price_tracking WHERE signal_id=s.id)
        GROUP BY s.pair, s.side HAVING total >= 2
        ORDER BY winrate DESC, total DESC LIMIT 30
    """)
    return jsonify(data)

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
    elif export_type == 'mechazos':
        signals = db_get("SELECT * FROM signals ORDER BY id DESC LIMIT 500")
        data = []
        for s in signals:
            r1 = get_reversion(s['id'], 60)
            r4 = get_reversion(s['id'], 240)
            r24 = get_reversion(s['id'], 1440)
            data.append({**s, 'rev_1h': r1, 'rev_4h': r4, 'rev_24h': r24, 'price_extremo': s['price_to']})
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
