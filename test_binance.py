#!/usr/bin/env python3
"""
Test de conectividad al WebSocket público de Binance.
Solo se conecta, escucha trades de BTC/USDT y los imprime.
No necesita API key. Si imprime trades, Binance es accesible desde tu VPS.

Uso en el contenedor:
    python3 test_binance.py
"""
import json
import websocket  # pip install websocket-client (ya lo tienes para Kraken)

# Stream de trades agregados de BTCUSDT (cada mensaje = una orden grande del taker)
URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
# Alternativa solo-datos por si la principal está restringida:
URL_ALT = "wss://data-stream.binance.vision/ws/btcusdt@aggTrade"

contador = {"n": 0}

def on_message(ws, message):
    data = json.loads(message)
    # Campos del aggTrade: p=precio, q=cantidad, m=true si el comprador es market maker
    precio = float(data.get("p", 0))
    cantidad = float(data.get("q", 0))
    usd = precio * cantidad
    lado = "VENTA" if data.get("m") else "COMPRA"  # m=True → el comprador es maker → fue una venta agresiva
    contador["n"] += 1
    print(f"  [{contador['n']:>2}] {lado:6} | {cantidad:.4f} BTC | {usd:,.0f} USD | precio {precio:,.1f}")
    if contador["n"] >= 15:
        print("\n✅ FUNCIONA — Binance es accesible desde este servidor. Recibidos 15 trades.")
        ws.close()

def on_error(ws, error):
    print(f"❌ ERROR: {error}")

def on_close(ws, code, msg):
    print(f"Conexión cerrada (code={code})")

def on_open(ws):
    print(f"🔌 Conectado a {ws.url}")
    print("Esperando trades de BTC/USDT...\n")

def probar(url):
    print(f"\n=== Probando {url} ===")
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=180, ping_timeout=10)

if __name__ == "__main__":
    contador["n"] = 0
    probar(URL)
    if contador["n"] == 0:
        print("\n⚠ La URL principal no recibió datos. Probando endpoint alternativo (data-stream.binance.vision)...")
        contador["n"] = 0
        probar(URL_ALT)
    if contador["n"] == 0:
        print("\n❌ Ninguna URL funcionó — Binance parece bloqueado desde este servidor (restricción geográfica).")
