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
DB_PATH = "/data/signals.db"

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
    conn.commit()
    conn.close()
    print(f"BD lista en: {DB_PATH}")

def save_signal(tradeDF, pair, volInEUR, priceDiff):
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
        print(f"Error guardando en BD: {e}")

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


def getEurWsnames(wsnames):
    # Solo pares con EUR — los más relevantes para detectar ballenas
    eurWsnames = {k: v for k, v in wsnames.items() if re.findall("/EUR$|^EUR/|/USD$|^USD/", v)}
    return(eurWsnames)


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
    if(zeros >= 3 and zeros < 6):
        return("{}K".format(round(x/1e3, 2)))
    elif(zeros >= 6 and zeros < 9):
        return("{}M".format(round(x/1e6, 2)))
    elif(zeros >= 9):
        return("{}B".format(round(x/1e9, 2)))
    else:
        return(x)


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
    # Suscribir en grupos de 50
    chunks = [pairsList[i:i+50] for i in range(0, len(pairsList), 50)]
    for chunk in chunks:
        msg = json.dumps({"event": "subscribe", "pair": chunk, "subscription": {"name": "trade"}})
        ws[0].send(msg)
        ws[1].send(msg)
        time.sleep(1)
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
    
    whaleSize = int(math.log10(volInEUR/1000)+1)
    whaleEmojis = "\U0001F433" * whaleSize
    
    if(priceDiff >= 5 and priceDiff < 10):
        changeEmoji = '\U0001F632'
    elif(priceDiff >= 10 and priceDiff < 20):
        changeEmoji = '\U0001F911'
    elif(priceDiff >= 20):
        changeEmoji = '\U0001F92F'
    else:
        changeEmoji = ""

    volInEUR = anotateVolume(volInEUR)
    segundaLinea = f"\n*{sign}{round(priceDiff, 2)}%* {changeEmoji} {whaleEmojis}: {volumeBase} {base} ({volumeTokenAnnotated} {token} {volInEUR}€)"

    firstDate = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[0])).strftime('%Y-%m-%d %H:%M:%S')
    lastDate  = datetime.fromtimestamp(pd.to_numeric(tradeDF["time"].iloc[-1])).strftime('%S')
    terceraLinea = f"\n{firstDate}::{lastDate}"

    fromPrice = tradeDF["price"].iloc[0]
    toPrice = tradeDF["price"].iloc[-1]
    url = f'[{pair}](https://cryptowat.ch/charts/KRAKEN:{tokenNormalized}-{baseNormalized}?period=1m)'
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
    
    quintaLinea = marginMessage
    tgMessage = f"{primeraLinea} {segundaLinea} {terceraLinea} {cuartaLinea} {quintaLinea}"
    return(tgMessage)


def telegram_bot_sendtext(bot_message):
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {
            'chat_id': bot_chatID,
            'parse_mode' : "Markdown",
            'text' : bot_message
            }
    response = requests.post(url, params = params)
    return response.json()


def connectTradeWS():
    init_db()
    print("Getting pairs...")
    pairs = getPairs()
    wsnames = getNamesForWS(pairs)

    # Solo pares EUR — evita saturar Kraken con 1500+ pares
    eurWsnames = getEurWsnames(wsnames)
    print(f"Following {len(eurWsnames)} EUR pairs (of {len(wsnames)} total)")

    eurPrices = getEurPrice(wsnames)
    pairsList = list(eurWsnames.values())

    print("Connecting to WebSocket...")
    ws = connectToWS(pairsList)
    print("Sent and Subscribed")
    print("Receiving...")
    telegram_bot_sendtext("\U0001F40D Levantado")

    while True:
        try:
            firstReceived = receiveSafeWS(ws)
            if firstReceived is None:
                raise Exception("receiveSafeWS returned None — reconnecting")
            result = list(json.loads(firstReceived))

            while type(result[0]) != int:
                firstReceived = receiveSafeWS(ws)
                if firstReceived is None:
                    raise Exception("receiveSafeWS returned None — reconnecting")
                result = list(json.loads(firstReceived))
            
            while len(result) == 1:
                firstReceived = receiveSafeWS(ws)
                if firstReceived is None:
                    raise Exception("receiveSafeWS returned None — reconnecting")
                result = list(json.loads(firstReceived))

            if len(result[1]) != 1:
                tradeDF = pd.DataFrame(result[1], columns=["price", "volume", "time", "side", "orderType", "misc"])
                tradeDF = tradeDF.sort_values(by=["time"])
                prices = pd.to_numeric(tradeDF["price"])
                priceDiff = abs(float((prices.iloc[0] - prices.iloc[-1]) * 100 / prices.iloc[0]))
                
                pair = result[3]
                volume = sum(pd.to_numeric(tradeDF["volume"]))
                volInEUR = volumeInEUR(wsnames, pair, volume, eurPrices)
                
                if(volInEUR == 0 or priceDiff > 3 and volInEUR > 2000):
                    priceDiff = round(priceDiff, 3)
                    print("\U0001F433", priceDiff, pair)
                    save_signal(tradeDF, pair, volInEUR, priceDiff)
                    TGmsg = createTGmessage(tradeDF, pair, volInEUR, priceDiff, wsnames, pairs)
                    telegram_bot_sendtext(TGmsg)
                else:
                    print(len(tradeDF), end=" ", flush=True)

            if(datetime.now().minute % 5 == 0 and datetime.now().second == 10):
                time.sleep(1)
                updatedPairs = getPairs()
                updatedWsnames = getNamesForWS(updatedPairs)
                updatedEurWsnames = getEurWsnames(updatedWsnames)
                if(len(updatedEurWsnames) != len(eurWsnames)):
                    print("New pairs. Updating...")
                    pairs = updatedPairs
                    wsnames = updatedWsnames
                    eurWsnames = updatedEurWsnames
                    ws[0].close()
                    ws[1].close()
                    connectTradeWS()

            if(datetime.now().second == 0):
                time.sleep(1)
                eurPrices = getEurPrice(wsnames)
                print("✓ ", end="")

        except Exception:
            traceback.print_exc()
            print("Disconnected. Trying...")
            telegram_bot_sendtext("\U0001F534 CAÍDO \U0001F534")
            time.sleep(5)
            connectTradeWS()


# Create msg for connection
connectTradeWS()
