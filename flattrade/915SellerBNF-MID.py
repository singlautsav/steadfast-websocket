# import sqlite3
import math
from datetime import date, timedelta, datetime
# import datetime
import json
from pya3 import *
import time
import psycopg2
from api_helper import NorenApiPy
# from api_helper import ShoonyaApiPy, get_time
import yaml
import pyotp
import pandas as pd
import requests
import time
import hashlib
import logging
import timeout_decorator
import requests
from urllib.parse import parse_qs,urlparse
from urllib.request import Request, urlopen
import os
import concurrent.futures
import zipfile
import io
import platform
import schedule
import redis
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi



global prevDate, hedgeList
SYMBOLDICT = {}
global CETsymBNF, CETsymMIDCP, PETsymBNF, PETsymMIDCP
global buyBookingPer, sellBookingPer
global tradingExpiry
global statusDay
global b1,b2,b3,b4,b5,s5,s1,s2,s3,s4,b6,s6
global strikeDate, strikeDatePrev, tokenList

global PEStrikePrice, CEStrikePrice
global quantityCE, quantityPE, quantityCEPrev, quantityPEPrev
global CEStrikePricePrevDay, PEStrikePricePrevDay
global conditionBuy, conditionSell
global LTP, socket_opened, subscribe_flag, subscribe_list, unsubscribe_list, tokenPair
tokenPair = []
lotSize = 50
# isTesting = False
# try:
fileMode = 'w'
if platform.platform().startswith("macOS"):
    basedir = './'
elif platform.platform().startswith("Linux"):
    basedir = '/home/us/AWS-Production/'
else:
    basedir = '''/home/ubuntu/AWS-Production/'''
    
try:
    if int(os.path.getsize(f"{basedir}/Logs/{str(date.today())}std.log")) > 1000:
        fileMode = 'a'
except:
    pass

logging.basicConfig(filename=f"{basedir}/Logs/{str(date.today())}std.log",
                    format='%(lineno)d - %(asctime)s - %(levelname)s - %(message)s', 
                    filemode=fileMode) 
logger=logging.getLogger() 
logger.setLevel(logging.INFO)

# '''load env variables'''
with open(f'{basedir}/cred.yml') as f:
    impo = yaml.load(f, Loader=yaml.FullLoader)

mongoURI = impo['mongoURI']
mongoIdentifier = impo['mongoIdentifierNavya']
TGToken1 = impo['TGToken1']
TGToken2 = impo['TGToken2']
chatID = impo['chatID']

url = 'https://prices.algotest.in/is-trading-holiday'
X = requests.get(url).json()
if X['answer']:
    raise ValueError('Trading Holiday')

client = MongoClient(mongoURI , server_api=ServerApi('1'))
for cred in client['creds']['flattrade'].find({'uniqueIdentifier':mongoIdentifier}):
    pass

user_id = cred['user_id']
prevDate = cred['prevDate']
strikeDate = cred['strikeDate']
expiryFut = '2022020202'
api = NorenApiPy()
userid = cred['user']
password = cred['pwd']
API_KEY = cred['apikey']
API_SECRET = cred['apisecret']
totp_key = cred['totp_key']
strikeDateBNF = cred['strikeDateBNF']
strikeDateMID = cred['strikeDateMID']
strikeDateBANKEX = cred['strikeDateBANKEX']
strikeDateSENSEX = cred['strikeDateSENSEX']
strikeDateNIFTY = cred['strikeDateNIFTY']
strikeDateFINNIFTY = cred['strikeDateFINNIFTY']
# generateStrikePriceData()

def updateMongo(value, updation):
    if value=='creds':
        client['creds']['flattrade'].update_one({'uniqueIdentifier':mongoIdentifier},{'$set':updation})

def reauth():
    headerJson ={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36","Referer":"https://auth.flattrade.in/"}
    sesUrl = 'https://authapi.flattrade.in/auth/session'
    passwordEncrpted =  hashlib.sha256(password.encode()).hexdigest()
    ses = requests.Session()
    res_pin = ses.post(sesUrl,headers=headerJson)
    sid = res_pin.text
    url2 = 'https://authapi.flattrade.in/ftauth'
    payload = {"UserName":userid,"Password":passwordEncrpted,"PAN_DOB":pyotp.TOTP(totp_key).now(),"App":"","ClientID":"","Key":"","APIKey":API_KEY,"Sid":sid}
    res2 = ses.post(url2, json=payload)
    reqcodeRes = res2.json()
    parsed = urlparse(reqcodeRes['RedirectURL'])
    reqCode = parse_qs(parsed.query)['code'][0]
    api_secret =API_KEY+ reqCode + API_SECRET
    api_secret =  hashlib.sha256(api_secret.encode()).hexdigest()
    payload = {"api_key":API_KEY, "request_code":reqCode, "api_secret":api_secret}
    url3 = 'https://authapi.flattrade.in/trade/apitoken'
    res3 = ses.post(url3, json=payload)
    usertoken = res3.json()['token']
    ret = api.set_session(userid= userid, password = password, usertoken= usertoken)
    cred['user_token'] = usertoken
    updateMongo('creds', cred)
    return ret

if cred['user_token'] == '':
    try:
        ret = reauth()
        print(cred['user_token'])
    except Exception as e:
        logger.error(e)
        message = f'''🔴 🔴 |  FT Login Error | Failed Login'''
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={message}"
        requests.get(url).json()
else:
    ret = api.set_session(userid= userid, password = password, usertoken= cred['user_token'])

global priceCollector
priceCollector = {}
tick_data = {}

def orderUpdate(tsym, avgprc, fillshares, remarks, trantype, exch, norenordno, message):
    logger.info(f'ORDER UPDATE {tsym, avgprc, fillshares, remarks, trantype, exch, norenordno}')
    try:
        if trantype=='S':
            if tsym in priceCollector.keys():
                priceCollector[tsym]['sellPrice'] = float(avgprc)
                priceCollector[tsym]['sellQty'] += int(fillshares)
            else:
                if remarks!='':
                    valuesX = eval(remarks)
                    priceCollector[tsym] = {'exchange': exch,'sellPrice': float(avgprc), 'sellQty': int(fillshares), 'buyPrice': 0.0, 'buyQty': 0, 'lotQty': valuesX[1], 'SL':False, 'halfProfit':False,}
        if trantype=='B':
            if tsym in priceCollector.keys():
                priceCollector[tsym]['buyPrice'] = float(avgprc)
                priceCollector[tsym]['buyQty'] += int(fillshares)
            else:
                if remarks!='':
                    valuesX = eval(remarks)
                    priceCollector[tsym] = {'exchange': exch,'buyPrice': float(avgprc), 'buyQty': int(fillshares), 'sellPrice': 0.0, 'sellQty': 0, 'lotQty': valuesX[1], 'SL':False, 'halfProfit':False}
            
        logger.info(f'PC & Quant {tsym, priceCollector[tsym], quantities}')
        with open(f'{basedir}/priceCollector.json', 'w') as f:
            json.dump(priceCollector, f)
        message = f"🟢 🟢 {tsym} | {fillshares} | {avgprc} | {str(datetime.now())}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()
        # r.publish('Order', message)
    except Exception as e:
        message = f"🔴 🔴 | Error {e} While Updating Order | {tsym} | {fillshares} | {avgprc} | {str(datetime.now())}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()
    message['algo'] = '915SellerHedge'
    client['creds']['orders'].insert_one(message)

def event_handler_order_update(message):
    # global priceCollector, quantities["FNF"], quantities['NIFTY'], quantities["SENSEX"], quantities["BNF"], quantities["MID"], qtyEXP
    try:
        if message['reporttype']== 'NewAck':
            return
        elif message['reporttype']== 'PendingNew':
            return
        elif message['reporttype']== 'New':
            return
        elif message['reporttype']== 'Fill':
            if message['status'] == 'COMPLETE':
                t = threading.Thread(target=orderUpdate, args=(message['tsym'],message['avgprc'], message['fillshares'], message['remarks'], message['trantype'], message['exch'], message['norenordno'], message))
                threads.append(t)
                t.start()
                # logger.info(message)
        else:
            raise ValueError((f"Order Rejected"))
    except Exception as e:
        logger.error(f'{e}, {message}')
        if 'rejreason' in message.keys():
            message = f"🔴 🔴 | Order Rejected | {message['rejreason']} | {e} | {message['remarks']}"
        elif 'reporttype' in message.keys():
            message = f"🔴 🔴 | Order Rejected | {message['reporttype']} | {e} | {message['remarks']}"
        else:
            message = f"🔴 🔴 | Order Rejected | {message['remarks']} | {e}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()

def takeTradeOld(symbol,openingPrice):
    global SYMBOLDICT
    global CETsymBNF, CETsymMIDCP, PETsymBNF, PETsymMIDCP, BNFOrders, midXOrders, bankexOrders, NFOrders, SENSEXOrders, FINNIFTYOrders
    global price
    # global isCompleted, traded['isNFTraded'], traded['isMIDTraded'], traded['isNFTraded'], traded['isNFTraded'], traded['isSENSEXTraded'], traded['isFNFTraded']
    global dfX, tokenPair, tokenList
    try:
        if symbol=="MIDCPNIFTY" and (not traded['isMIDTraded']):
            try:
                results = []
                traded['isMIDTraded'] = True
                CEStrikePrice = openingPrice + 50
                CEStrikePrice = 25 * round(CEStrikePrice / 25)
                PEStrikePrice = openingPrice - 50
                PEStrikePrice = 25 * round(PEStrikePrice / 25)
                CETsymMIDCP     = dfX[(dfX['Symbol']=='MIDCPNIFTY') & (dfX['Expiry']==strikeDateMID) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                
                CETsymMIDCPToken = CETsymMIDCP['token'].values[0]
                midLotSize = int(CETsymMIDCP['lotSize'].values[0])
                CETsymMIDCP = CETsymMIDCP['tsym'].values[0]
                print(midLotSize)
                PETsymMIDCP     = dfX[(dfX['Symbol']=='MIDCPNIFTY') & (dfX['Expiry']==strikeDateMID) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymMIDCPToken = PETsymMIDCP['token'].values[0]
                PETsymMIDCP = PETsymMIDCP['tsym'].values[0]
                tokenPair.append([(str(CETsymMIDCPToken), CETsymMIDCP), (str(PETsymMIDCPToken), PETsymMIDCP)])
                tokenList.append('NFO|'+str(CETsymMIDCPToken))
                tokenList.append('NFO|'+str(PETsymMIDCPToken))
                api.subscribe(['NFO|'+str(CETsymMIDCPToken), 'NFO|'+str(PETsymMIDCPToken)])
                
                if strikeDateMID==str(datetime.now().date()):
                    CETsymMIDCPEXP     = dfX[(dfX['Symbol']=='MIDCPNIFTY') & (dfX['Expiry']==strikeDateMID) & (dfX['StrikePrice']==int(CEStrikePrice)+25) & (dfX['optt']=='CE')]
                    CETsymMIDCPEXPToken = CETsymMIDCPEXP['token'].values[0]
                    CETsymMIDCPEXP = CETsymMIDCPEXP['tsym'].values[0]
                    
                    PETsymMIDCPEXP     = dfX[(dfX['Symbol']=='MIDCPNIFTY') & (dfX['Expiry']==strikeDateMID) & (dfX['StrikePrice']==int(PEStrikePrice)-25) & (dfX['optt']=='PE')]
                    PETsymMIDCPEXPToken = PETsymMIDCPEXP['token'].values[0]
                    PETsymMIDCPEXP = PETsymMIDCPEXP['tsym'].values[0]
                    
                    tokenPair.append([(str(CETsymMIDCPEXPToken), CETsymMIDCPEXP), (str(PETsymMIDCPEXPToken), PETsymMIDCPEXP)])
                    tokenList.append('NFO|'+str(CETsymMIDCPEXPToken))
                    tokenList.append('NFO|'+str(PETsymMIDCPEXPToken))
                    api.subscribe(['NFO|'+str(CETsymMIDCPEXPToken), 'NFO|'+str(PETsymMIDCPEXPToken)])
                    
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymMIDCP, (quantities["MID"])*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{CETsymMIDCP, quantities["MID"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymMIDCP, (quantities["MID"])*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{PETsymMIDCP, quantities["MID"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymMIDCPEXP, quantities["MIDEXP"]*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{CETsymMIDCPEXP, quantities["MIDEXP"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymMIDCPEXP, quantities["MIDEXP"]*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{PETsymMIDCPEXP, quantities["MIDEXP"]}'),
                        ]
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        midXOrders.append(('S',CETsymMIDCP,quantities["MID"]*midLotSize ,'NFO',quantities["MID"]))
                        midXOrders.append(('S',PETsymMIDCP,quantities["MID"]*midLotSize ,'NFO',quantities["MID"]))
                        midXOrders.append(('S',CETsymMIDCPEXP,quantities["MIDEXP"]*midLotSize ,'NFO',quantities["MIDEXP"]))
                        midXOrders.append(('S',PETsymMIDCPEXP ,quantities["MIDEXP"]*midLotSize ,'NFO',quantities["MIDEXP"]))
                        with open(f'{basedir}/midList.json', 'w') as f:
                            json.dump(midXOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isMIDTraded': True, 'lastUpdatedTime': datetime.now()}})
                else:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymMIDCP, quantities["MID"]*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{CETsymMIDCP, quantities["MID"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymMIDCP, quantities["MID"]*midLotSize , 0, 'MKT', 0, None, 'DAY', f'{PETsymMIDCP, quantities["MID"]}')
                        ]
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders] 
                        midXOrders.append(('S',CETsymMIDCP,quantities["MID"]*midLotSize ,'NFO',quantities["MID"]))
                        midXOrders.append(('S',PETsymMIDCP,quantities["MID"]*midLotSize ,'NFO',quantities["MID"]))
                        # midXOrders = [(CETsymMIDCP,quantities["MID"]*midLotSize , 'NFO'),(PETsymMIDCP,quantities["MID"]*midLotSize ,'NFO')]
                        with open(f'{basedir}/midList.json', 'w') as f:
                            json.dump(midXOrders, f)
                        # results = [order.result() for order in orders] 
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isMIDTraded': True, 'lastUpdatedTime': datetime.now()}})
                logger.info(f'{CETsymMIDCP, PETsymMIDCP}')    
                logger.info(f'midXOrders {midXOrders}')
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isMIDTraded'] = False
        elif symbol=="NIFTY" and (not traded['isNFTraded']):
            try:
                results = []
                traded['isNFTraded'] = True
                # openingPrice3 = float(SYMBOLDICT[key]['lp'])
                CEStrikePrice = openingPrice + 100
                CEStrikePrice = 50 * round(CEStrikePrice / 50)
                PEStrikePrice = openingPrice - 100
                PEStrikePrice = 50 * round(PEStrikePrice / 50)
                # print(f'BANKNIFTY - {CEStrikePrice}, {PEStrikePrice}')
                CETsymNIFTY      = dfX[(dfX['Symbol']=='NIFTY') & (dfX['Expiry']==strikeDateNIFTY) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                CETsymNIFTYToken = CETsymNIFTY['token'].values[0]
                nfLotSize = int(CETsymNIFTY['lotSize'].values[0])
                CETsymNIFTY = CETsymNIFTY['tsym'].values[0]
                
                
                PETsymNIFTY      = dfX[(dfX['Symbol']=='NIFTY') & (dfX['Expiry']==strikeDateNIFTY) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymNIFTYToken = PETsymNIFTY['token'].values[0]
                PETsymNIFTY = PETsymNIFTY['tsym'].values[0]
                tokenPair.append([(str(CETsymNIFTYToken), CETsymNIFTY), (str(PETsymNIFTYToken), PETsymNIFTY)])
                tokenList.append('NFO|'+str(CETsymNIFTYToken))
                tokenList.append('NFO|'+str(PETsymNIFTYToken))
                api.subscribe(['NFO|'+str(CETsymNIFTYToken), 'NFO|'+str(PETsymNIFTYToken)])
                
                if strikeDateNIFTY==str(datetime.now().date()) and tradingExpiry:
                    CETsymNIFTYEXP = dfX[(dfX['Symbol']=='NIFTY') & (dfX['Expiry']==strikeDateNIFTY) & (dfX['StrikePrice']==int(CEStrikePrice)+50) & (dfX['optt']=='CE')]
                    CETsymNIFTYEXPToken = CETsymNIFTYEXP['token'].values[0]
                    CETsymNIFTYEXP = CETsymNIFTYEXP['tsym'].values[0]
                    
                    PETsymNIFTYEXP = dfX[(dfX['Symbol']=='NIFTY') & (dfX['Expiry']==strikeDateNIFTY) & (dfX['StrikePrice']==int(PEStrikePrice)-50) & (dfX['optt']=='PE')]
                    PETsymNIFTYEXPToken = PETsymNIFTYEXP['token'].values[0]
                    PETsymNIFTYEXP = PETsymNIFTYEXP['tsym'].values[0]
                    tokenPair.append([(str(CETsymNIFTYEXPToken), CETsymNIFTYEXP),(str(PETsymNIFTYEXPToken), PETsymNIFTYEXP)])
                    tokenList.append('NFO|'+str(CETsymNIFTYEXPToken))
                    tokenList.append('NFO|'+str(PETsymNIFTYEXPToken))
                    api.subscribe(['NFO|'+str(CETsymNIFTYEXPToken), 'NFO|'+str(PETsymNIFTYEXPToken)])
                    
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymNIFTY, (quantities["NF"])*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymNIFTY, quantities["NF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymNIFTY, (quantities["NF"])*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymNIFTY, quantities["NF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymNIFTYEXP, (quantities["NFEXP"])*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymNIFTYEXP, quantities["NFEXP"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymNIFTYEXP, (quantities["NFEXP"])*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymNIFTYEXP, quantities["NFEXP"]}'),
                        ]
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        NFOrders.append(('S',CETsymNIFTY,quantities["NF"]*nfLotSize, 'NFO', quantities["NF"]))
                        NFOrders.append(('S',PETsymNIFTY,quantities["NF"]*nfLotSize, 'NFO', quantities["NF"]))
                        NFOrders.append(('S',CETsymNIFTYEXP,(quantities["NFEXP"])*nfLotSize, 'NFO', quantities["NFEXP"]))
                        NFOrders.append(('S',PETsymNIFTYEXP,(quantities["NFEXP"])*nfLotSize, 'NFO', quantities["NFEXP"]))
                        with open(f'{basedir}/NFList.json', 'w') as f:
                            json.dump(NFOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                else:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymNIFTY, quantities["NF"]*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymNIFTY, quantities["NF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymNIFTY, quantities["NF"]*nfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymNIFTY, quantities["NF"]}')
                        ]
                        # Wait for all orders to complete (this will not block)
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        NFOrders.append(('S',CETsymNIFTY,quantities["NF"]*nfLotSize, 'NFO', quantities["NF"]))
                        NFOrders.append(('S',PETsymNIFTY,quantities["NF"]*nfLotSize, 'NFO', quantities["NF"]))
                        with open(f'{basedir}/NFList.json', 'w') as f:
                            json.dump(NFOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                # logger.info(f'{CETsymNIFTY, PETsymNIFTY}')
                logger.info(f"{NFOrders}")
                traded['isNFTraded'] = True
                
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isNFTraded'] = False
        elif symbol=="BANKNIFTY" and (not traded['isBNFTraded']):
            try:
                results = []
                traded['isBNFTraded'] = True
                # openingPrice1 = float(SYMBOLDICT[key]['lp'])
                CEStrikePrice = openingPrice + 200
                CEStrikePrice = 100 * round(CEStrikePrice / 100)
                PEStrikePrice = openingPrice - 200
                PEStrikePrice = 100 * round(PEStrikePrice / 100)
                # print(f'BANKNIFTY - {CEStrikePrice}, {PEStrikePrice}')
                CETsymBNF      = dfX[(dfX['Symbol']=='BANKNIFTY') & (dfX['Expiry']==strikeDateBNF) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                CETsymBNFToken = CETsymBNF['token'].values[0]
                bnfLotSize = int(CETsymBNF['lotSize'].values[0])
                CETsymBNF       = CETsymBNF['tsym'].values[0]
                
                
                PETsymBNF      = dfX[(dfX['Symbol']=='BANKNIFTY') & (dfX['Expiry']==strikeDateBNF) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymBNFToken = PETsymBNF['token'].values[0]
                PETsymBNF = PETsymBNF['tsym'].values[0]
                
                tokenPair.append([(str(CETsymBNFToken), CETsymBNF), (str(PETsymBNFToken), PETsymBNF)])
                tokenList.append('NFO|'+str(CETsymBNFToken))
                tokenList.append('NFO|'+str(PETsymBNFToken))
                api.subscribe(['NFO|'+str(CETsymBNFToken), 'NFO|'+str(PETsymBNFToken)])
                
                if strikeDateBNF==str(datetime.now().date()) and tradingExpiry:
                    CETsymBNFEXP = dfX[(dfX['Symbol']=='BANKNIFTY') & (dfX['Expiry']==strikeDateBNF) & (dfX['StrikePrice']==int(CEStrikePrice)+100) & (dfX['optt']=='CE')]
                    CETsymBNFEXPToken = CETsymBNFEXP['token'].values[0]
                    CETsymBNFEXP = CETsymBNFEXP['tsym'].values[0]
                    PETsymBNFEXP = dfX[(dfX['Symbol']=='BANKNIFTY') & (dfX['Expiry']==strikeDateBNF) & (dfX['StrikePrice']==int(PEStrikePrice)-100) & (dfX['optt']=='PE')]
                    PETsymBNFEXPToken = PETsymBNFEXP['token'].values[0]
                    PETsymBNFEXP = PETsymBNFEXP['tsym'].values[0]
                    tokenPair.append([(str(CETsymBNFEXPToken), CETsymBNFEXP), (str(PETsymBNFEXPToken), PETsymBNFEXP)])
                    tokenList.append('NFO|'+str(CETsymBNFEXPToken))
                    tokenList.append('NFO|'+str(PETsymBNFEXPToken))
                    api.subscribe(['NFO|'+str(CETsymBNFEXPToken), 'NFO|'+str(PETsymBNFEXPToken)])
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymBNF, (quantities["BNF"])*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymBNF,quantities["BNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymBNF, (quantities["BNF"])*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymBNF,quantities["BNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymBNFEXP, (quantities["BNFEXP"])*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymBNFEXP,quantities["BNFEXP"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymBNFEXP, (quantities["BNFEXP"])*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymBNFEXP,quantities["BNFEXP"]}'),
                        ]
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        BNFOrders.append(('S',CETsymBNF,quantities["BNF"]*bnfLotSize,'NFO',quantities["BNF"]))
                        BNFOrders.append(('S',PETsymBNF,quantities["BNF"]*bnfLotSize,'NFO',quantities["BNF"]))
                        BNFOrders.append(('S',CETsymBNFEXP,quantities["BNFEXP"]*bnfLotSize,'NFO',quantities["BNFEXP"]))
                        BNFOrders.append(('S',PETsymBNFEXP,quantities["BNFEXP"]*bnfLotSize,'NFO',quantities["BNFEXP"]))
                        with open(f'{basedir}/bankList.json', 'w') as f:
                            json.dump(BNFOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isBNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                else:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymBNF, quantities["BNF"]*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymBNF,quantities["BNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymBNF, quantities["BNF"]*bnfLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymBNF,quantities["BNF"]}')
                        ]
                        # Wait for all orders to complete (this will not block)
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        
                        BNFOrders.append(('S',CETsymBNF,quantities["BNF"]*bnfLotSize,'NFO',quantities["BNF"]))
                        BNFOrders.append(('S',PETsymBNF,quantities["BNF"]*bnfLotSize,'NFO',quantities["BNF"]))
                        with open(f'{basedir}/bankList.json', 'w') as f:
                            json.dump(BNFOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isBNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                # logger.info(f'{CETsymBNF, PETsymBNF}')
                logger.info(f"{BNFOrders}")
                traded['isBNFTraded'] = True
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isBNFTraded'] = False
        elif symbol=="SENSEX" and (not traded['isSENSEXTraded']):
            try:
                results = []
                traded['isSENSEXTraded']= True
                # openingPrice5 = float(SYMBOLDICT[key]['lp'])
                # print("OpeningPrice for SENSEX is: ", openingPrice)
                CEStrikePrice = openingPrice + 200
                CEStrikePrice = 100 * round(CEStrikePrice / 100)
                PEStrikePrice = openingPrice - 200
                PEStrikePrice = 100 * round(PEStrikePrice / 100)
                
                # print(f'SENSEX - {CEStrikePrice}, {PEStrikePrice}')
                CETsymSENSEX      = dfX[(dfX['Symbol']=="SENSEX") & (dfX['Expiry']==strikeDateSENSEX) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                CETsymSENSEXToken = CETsymSENSEX['token'].values[0]
                sensexLotSize = int(CETsymSENSEX['lotSize'].values[0])
                CETsymSENSEX = CETsymSENSEX['tsym'].values[0]
                
                
                PETsymSENSEX      = dfX[(dfX['Symbol']=="SENSEX") & (dfX['Expiry']==strikeDateSENSEX) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymSENSEXToken = PETsymSENSEX['token'].values[0]
                PETsymSENSEX = PETsymSENSEX['tsym'].values[0]
                tokenPair.append([(str(CETsymSENSEXToken), CETsymSENSEX), (str(PETsymSENSEXToken),PETsymSENSEX)])
                tokenList.append('BFO|'+str(CETsymSENSEXToken))
                tokenList.append('BFO|'+str(PETsymSENSEXToken))
                api.subscribe(['BFO|'+str(CETsymSENSEXToken), 'BFO|'+str(PETsymSENSEXToken)])
                
                CETsymSENSEXEXP      = dfX[(dfX['Symbol']=="SENSEX") & (dfX['Expiry']==strikeDateSENSEX) & (dfX['StrikePrice']==int(CEStrikePrice)+100) & (dfX['optt']=='CE')]
                CETsymSENSEXEXPToken = CETsymSENSEXEXP['token'].values[0]
                CETsymSENSEXEXP = CETsymSENSEXEXP['tsym'].values[0]
                PETsymSENSEXEXP      = dfX[(dfX['Symbol']=="SENSEX") & (dfX['Expiry']==strikeDateSENSEX) & (dfX['StrikePrice']==int(PEStrikePrice)-100) & (dfX['optt']=='PE')]
                PETsymSENSEXEXPToken = PETsymSENSEXEXP['token'].values[0]
                PETsymSENSEXEXP = PETsymSENSEXEXP['tsym'].values[0]
                tokenPair.append([(str(CETsymSENSEXEXPToken), CETsymSENSEXEXP), (str(PETsymSENSEXEXPToken), PETsymSENSEXEXP)])
                tokenList.append('BFO|'+str(CETsymSENSEXEXPToken))
                tokenList.append('BFO|'+str(PETsymSENSEXEXPToken))
                api.subscribe(['BFO|'+str(CETsymSENSEXEXPToken), 'BFO|'+str(PETsymSENSEXEXPToken)])
                
                    
                logger.info(f'{CETsymSENSEX, PETsymSENSEX,CETsymSENSEXEXP, PETsymSENSEXEXP }')
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    orders = [
                        executor.submit(place_order, api, 'S', 'M', 'BFO', CETsymSENSEX, quantities["SENSEX"]*sensexLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSENSEX, quantities["SENSEX"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', PETsymSENSEX, quantities["SENSEX"]*sensexLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSENSEX, quantities["SENSEX"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', CETsymSENSEXEXP, quantities["SENSEXEXP"]*sensexLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSENSEXEXP, quantities["SENSEXEXP"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', PETsymSENSEXEXP, quantities["SENSEXEXP"]*sensexLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSENSEXEXP, quantities["SENSEXEXP"]}'),
                    ]
                    
                    concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                    results = [order.result() for order in orders]
                    SENSEXOrders.append(('S',CETsymSENSEX,quantities["SENSEX"]*sensexLotSize,'BFO',quantities["SENSEX"]))
                    SENSEXOrders.append(('S',PETsymSENSEX,quantities["SENSEX"]*sensexLotSize,'BFO',quantities["SENSEX"]))
                    SENSEXOrders.append(('S',CETsymSENSEXEXP,quantities["SENSEXEXP"]*sensexLotSize,'BFO',quantities["SENSEXEXP"]))
                    SENSEXOrders.append(('S',PETsymSENSEXEXP,quantities["SENSEXEXP"]*sensexLotSize,'BFO',quantities["SENSEXEXP"]))
                    
                    
                    with open(f'{basedir}/SENSEXList.json', 'w') as f:
                        json.dump(NFOrders, f)
                        # logger.info("3. SENSEX Orders are placed, NFlist updated No errors yet")
                    # logger.info(f"{results}")
                    client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isSENSEXTraded': True, 'lastUpdatedTime': datetime.now()}})
                traded['isSENSEXTraded'] = True
                logger.info(f"{SENSEXOrders}")
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isSENSEXTraded']= False
        elif symbol=="BANKEX" and (not traded['isBANKEXTraded']):
            try:
                results = []
                traded['isBANKEXTraded'] =  True
                # openingPrice4 = float(SYMBOLDICT[key]['lp'])
                CEStrikePrice = openingPrice + 200
                CEStrikePrice = 100 * round(CEStrikePrice / 100)
                PEStrikePrice = openingPrice - 200
                PEStrikePrice = 100 * round(PEStrikePrice / 100)
                # print(f'BANKNIFTY - {CEStrikePrice}, {PEStrikePrice}')
                CETsymBANKEX      = dfX[(dfX['Symbol']=="BANKEX") & (dfX['Expiry']==strikeDateBANKEX) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                CETsymBANKEXToken = CETsymBANKEX['token'].values[0]
                bankexLotSize = int(CETsymBANKEX['lotSize'].values[0])
                CETsymBANKEX = CETsymBANKEX['tsym'].values[0]
                
                
                PETsymBANKEX      = dfX[(dfX['Symbol']=="BANKEX") & (dfX['Expiry']==strikeDateBANKEX) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymBANKEXToken = PETsymBANKEX['token'].values[0]
                PETsymBANKEX = PETsymBANKEX['tsym'].values[0]
                tokenPair.append([(str(CETsymBANKEXToken), CETsymBANKEX), (str(PETsymBANKEXToken), PETsymBANKEX)])
                tokenList.append('BFO|'+str(CETsymBANKEXToken))
                tokenList.append('BFO|'+str(PETsymBANKEXToken))
                api.subscribe(['BFO|'+str(CETsymBANKEXToken), 'BFO|'+str(PETsymBANKEXToken)])
                
                CETsymBANKEXEXP      = dfX[(dfX['Symbol']=="BANKEX") & (dfX['Expiry']==strikeDateBANKEX) & (dfX['StrikePrice']==int(CEStrikePrice)+100) & (dfX['optt']=='CE')]
                CETsymBANKEXEXPToken = CETsymBANKEXEXP['token'].values[0]
                CETsymBANKEXEXP = CETsymBANKEXEXP['tsym'].values[0]
                
                PETsymBANKEXEXP      = dfX[(dfX['Symbol']=="BANKEX") & (dfX['Expiry']==strikeDateBANKEX) & (dfX['StrikePrice']==int(PEStrikePrice)-100) & (dfX['optt']=='PE')]
                PETsymBANKEXEXPToken = PETsymBANKEXEXP['token'].values[0]
                PETsymBANKEXEXP = PETsymBANKEXEXP['tsym'].values[0]
                tokenPair.append([(str(CETsymBANKEXEXPToken), CETsymBANKEXEXP), (str(PETsymBANKEXEXPToken), PETsymBANKEXEXP)])
                tokenList.append('BFO|'+str(CETsymBANKEXEXPToken))
                tokenList.append('BFO|'+str(PETsymBANKEXEXPToken))
                api.subscribe(['BFO|'+str(CETsymBANKEXEXPToken), 'BFO|'+str(PETsymBANKEXEXPToken)])
                logger.info(f'{CETsymBANKEX, PETsymBANKEX,CETsymBANKEXEXP, PETsymBANKEXEXP}')
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    orders = [
                        executor.submit(place_order, api, 'S', 'M', 'BFO', CETsymBANKEX, quantities["BANKEX"]*bankexLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymBANKEX, quantities["BANKEX"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', PETsymBANKEX, quantities["BANKEX"]*bankexLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymBANKEX, quantities["BANKEX"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', CETsymBANKEXEXP, quantities["BANKEX"]*bankexLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymBANKEXEXP, quantities["BANKEX"]}'),
                        executor.submit(place_order, api, 'S', 'M', 'BFO', PETsymBANKEXEXP, quantities["BANKEX"]*bankexLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymBANKEXEXP, quantities["BANKEX"]}'),
                    ]
                    concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                    # logger.info("1. BANKEX Orders are placed No errors yet")
                    results = [order.result() for order in orders]
                    
                    BANKEXOrders.append(('S',CETsymBANKEX,quantities["BANKEX"]*bankexLotSize,'BFO',quantities["BANKEX"]))
                    BANKEXOrders.append(('S',PETsymBANKEX,quantities["BANKEX"]*bankexLotSize,'BFO',quantities["BANKEX"]))
                    BANKEXOrders.append(('S',CETsymBANKEXEXP,quantities["BANKEXEXP"]*bankexLotSize,'BFO',quantities["BANKEXEXP"]))
                    BANKEXOrders.append(('S',PETsymBANKEXEXP,quantities["BANKEXEXP"]*bankexLotSize,'BFO',quantities["BANKEXEXP"]))
                    
                    with open(f'{basedir}/bankexList.json', 'w') as f:
                        json.dump(BNFOrders, f)
                        # logger.info("3. BANKEX Orders are placed, JSON updated No errors yet")
                    client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isBANKEXTraded': True, 'lastUpdatedTime': datetime.now()}})
                    # logger.info(f"{results}")
                traded['isBANKEXTraded'] = True
                logger.info(f"{BANKEXOrders}")
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isBANKEXTraded'] = False
        elif symbol=='FINNIFTY' and (not traded['isFNFTraded']):
            try:
                results = []
                traded['isFNFTraded'] = True
                # openingPrice1 = float(SYMBOLDICT[key]['lp'])
                CEStrikePrice = openingPrice + 50
                CEStrikePrice = 50 * round(CEStrikePrice / 50)
                PEStrikePrice = openingPrice - 50
                PEStrikePrice = 50 * round(PEStrikePrice / 50)
                # print(f'BANKNIFTY - {CEStrikePrice}, {PEStrikePrice}')
                CETsymFINNIFTY      = dfX[(dfX['Symbol']=='FINNIFTY') & (dfX['Expiry']==strikeDateFINNIFTY) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
                CETsymFINNIFTYToken = CETsymFINNIFTY['token'].values[0]
                finniftyLotSize = int(CETsymFINNIFTY['lotSize'].values[0])
                CETsymFINNIFTY       = CETsymFINNIFTY['tsym'].values[0]
                
                
                PETsymFINNIFTY      = dfX[(dfX['Symbol']=='FINNIFTY') & (dfX['Expiry']==strikeDateFINNIFTY) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
                PETsymFINNIFTYToken = PETsymFINNIFTY['token'].values[0]
                PETsymFINNIFTY = PETsymFINNIFTY['tsym'].values[0]
                
                tokenPair.append([(str(CETsymFINNIFTYToken), CETsymFINNIFTY), (str(PETsymFINNIFTYToken), PETsymFINNIFTY)])
                tokenList.append('NFO|'+str(CETsymFINNIFTYToken))
                tokenList.append('NFO|'+str(PETsymFINNIFTYToken))
                api.subscribe(['NFO|'+str(CETsymFINNIFTYToken), 'NFO|'+str(PETsymFINNIFTYToken)])
                
                if strikeDateFINNIFTY==str(datetime.now().date()) and tradingExpiry:
                    CETsymFINNIFTYEXP = dfX[(dfX['Symbol']=='FINNIFTY') & (dfX['Expiry']==strikeDateFINNIFTY) & (dfX['StrikePrice']==int(CEStrikePrice)+50) & (dfX['optt']=='CE')]
                    CETsymFINNIFTYEXPToken = CETsymFINNIFTYEXP['token'].values[0]
                    CETsymFINNIFTYEXP = CETsymFINNIFTYEXP['tsym'].values[0]
                    PETsymFINNIFTYEXP = dfX[(dfX['Symbol']=='FINNIFTY') & (dfX['Expiry']==strikeDateFINNIFTY) & (dfX['StrikePrice']==int(PEStrikePrice)-50) & (dfX['optt']=='PE')]
                    PETsymFINNIFTYEXPToken = PETsymFINNIFTYEXP['token'].values[0]
                    PETsymFINNIFTYEXP = PETsymFINNIFTYEXP['tsym'].values[0]
                    tokenPair.append([(str(CETsymFINNIFTYEXPToken), CETsymFINNIFTYEXP), (str(PETsymFINNIFTYEXPToken), PETsymFINNIFTYEXP)])
                    tokenList.append('NFO|'+str(CETsymFINNIFTYEXPToken))
                    tokenList.append('NFO|'+str(PETsymFINNIFTYEXPToken))
                    api.subscribe(['NFO|'+str(CETsymFINNIFTYEXPToken), 'NFO|'+str(PETsymFINNIFTYEXPToken)])
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymFINNIFTY, (quantities["FNF"])*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymFINNIFTY,quantities["FNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymFINNIFTY, (quantities["FNF"])*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymFINNIFTY,quantities["FNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymFINNIFTYEXP, (quantities["FNFEXP"])*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymFINNIFTYEXP,quantities["FNFEXP"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymFINNIFTYEXP, (quantities["FNFEXP"])*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymFINNIFTYEXP,quantities["FNFEXP"]}'),
                        ]
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        FINNIFTYOrders.append(('S',CETsymFINNIFTY,quantities["FNF"]*finniftyLotSize,'NFO',quantities["FNF"]))
                        FINNIFTYOrders.append(('S',PETsymFINNIFTY,quantities["FNF"]*finniftyLotSize,'NFO',quantities["FNF"]))
                        FINNIFTYOrders.append(('S',CETsymFINNIFTYEXP,quantities["FNFEXP"]*finniftyLotSize,'NFO',quantities["FNFEXP"]))
                        FINNIFTYOrders.append(('S',PETsymFINNIFTYEXP,quantities["FNFEXP"]*finniftyLotSize,'NFO',quantities["FNFEXP"]))
                        with open(f'{basedir}/FINNIFTYList.json', 'w') as f:
                            json.dump(FINNIFTYOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isFNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                else:
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        orders = [
                            executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymFINNIFTY, quantities["FNF"]*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymFINNIFTY,quantities["FNF"]}'),
                            executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymFINNIFTY, quantities["FNF"]*finniftyLotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymFINNIFTY,quantities["FNF"]}')
                        ]
                        # Wait for all orders to complete (this will not block)
                        concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                        results = [order.result() for order in orders]
                        FINNIFTYOrders.append(('S',CETsymFINNIFTY,quantities["FNF"]*finniftyLotSize,'NFO',quantities["FNF"]))
                        FINNIFTYOrders.append(('S',PETsymFINNIFTY,quantities["FNF"]*finniftyLotSize,'NFO',quantities["FNF"]))
                        with open(f'{basedir}/FINNIFTYList.json', 'w') as f:
                            json.dump(FINNIFTYOrders, f)
                        logger.info(f"{results}")
                        client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {'isFNFTraded': True, 'lastUpdatedTime': datetime.now()}})
                # logger.info(f'{CETsymFINNIFTY, PETsymFINNIFTY}')
                logger.info(f"{FINNIFTYOrders}")
                traded['isFNFTraded'] = True
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump(tokenList, f)
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump(tokenPair, f)
            except Exception as e:
                logger.error(f'{e}')
                message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
                url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
                if len(results)>0:
                    pass
                else:  
                    traded['isFNFTraded'] = False
        
    
    except Exception as e:
        logger.error(f'{e}')
        message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        # r.set('error', message)
        # r.set('errorTime', time.time())
        requests.get(url).json()
        # logger.info(message)
        # os.system('say "Error"')

'''belowCode has been sunset'''
def takeTrade(symbol, openingPrice, adder, expadder, modulus, shortX, strikeDateSymbol, qtyEXP, tradingExpiry):
    global tokenList, tokenPair
    try:
        results = []
        isSymbol = 'is'+shortX+'Traded'
        traded[isSymbol] = True
        # variableToBeUpdated = True
        CEStrikePrice = openingPrice + adder
        CEStrikePrice = modulus * round(CEStrikePrice / modulus)
        PEStrikePrice = openingPrice - adder
        PEStrikePrice = modulus * round(PEStrikePrice / modulus)
        logger.info(f'{CEStrikePrice, PEStrikePrice, symbol, strikeDateSymbol}')
        CETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
        CETsymSymbolToken = CETsymSymbol['token'].values[0]
        lotSize = int(CETsymSymbol['lotSize'].values[0])
        CETsymSymbol       = CETsymSymbol['tsym'].values[0]
        PETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
        PETsymSymbolToken = PETsymSymbol['token'].values[0]
        PETsymSymbol = PETsymSymbol['tsym'].values[0]
        logger.info(f'{CETsymSymbol, PETsymSymbol}')
        tokenPair.append([(str(CETsymSymbolToken), CETsymSymbol), (str(PETsymSymbolToken), PETsymSymbol)])
        tokenList.append('NFO|'+str(CETsymSymbolToken))
        tokenList.append('NFO|'+str(PETsymSymbolToken))
        api.subscribe(['NFO|'+str(CETsymSymbolToken), 'NFO|'+str(PETsymSymbolToken)])
        
        if strikeDateSymbol==str(datetime.now().date()) and tradingExpiry:
            CETsymSymbolEXP = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(CEStrikePrice)+expadder) & (dfX['optt']=='CE')]
            CETsymSymbolEXPToken = CETsymSymbolEXP['token'].values[0]
            CETsymSymbolEXP = CETsymSymbolEXP['tsym'].values[0]
            PETsymSymbolEXP = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(PEStrikePrice)-expadder) & (dfX['optt']=='PE')]
            PETsymSymbolEXPToken = PETsymSymbolEXP['token'].values[0]
            PETsymSymbolEXP = PETsymSymbolEXP['tsym'].values[0]
            tokenPair.append([(str(CETsymSymbolEXPToken), CETsymSymbolEXP), (str(PETsymSymbolEXPToken), PETsymSymbolEXP)])
            tokenList.append('NFO|'+str(CETsymSymbolEXPToken))
            tokenList.append('NFO|'+str(PETsymSymbolEXPToken))
            api.subscribe(['NFO|'+str(CETsymSymbolEXPToken), 'NFO|'+str(PETsymSymbolEXPToken)])
            with concurrent.futures.ThreadPoolExecutor() as executor:
                
                orders = [
                    executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymSymbol, (quantities[shortX])*lotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSymbol,quantities[shortX], "CALL"}'),
                    executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymSymbol, (quantities[shortX])*lotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSymbol,quantities[shortX], "PUT"}'),
                    executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymSymbolEXP, (qtyEXP)*lotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSymbolEXP,qtyEXP, "CALL"}'),
                    executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymSymbolEXP, (qtyEXP)*lotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSymbolEXP,qtyEXP, "PUT"}')
                    
                ]
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                results = [order.result() for order in orders]
                client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {isSymbol: True, 'lastUpdatedTime': datetime.now()}})
                logger.info(f"{results}")
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                orders = [
                    executor.submit(place_order, api, 'S', 'M', 'NFO', CETsymSymbol, quantities[shortX]*lotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSymbol,quantities[shortX], "CALL"}'),
                    executor.submit(place_order, api, 'S', 'M', 'NFO', PETsymSymbol, quantities[shortX]*lotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSymbol,quantities[shortX], "PUT"}')
                ]
                # Wait for all orders to complete (this will not block)
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                results = [order.result() for order in orders]
                client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {isSymbol: True, 'lastUpdatedTime': datetime.now()}})
                logger.info(f"{results}")
        # logger.info(f'{CETsymSymbol, PETsymSymbol}')
        # logger.info(f"{SymbolOrders}")
        # logger.info()
        traded[isSymbol] = True
        with open(f'{basedir}/tokenList.json', 'w') as f:
            json.dump(tokenList, f)
        with open(f'{basedir}/tokenPair.json', 'w') as f:
            json.dump(tokenPair, f)
    except Exception as e:
        logger.error(f'{e}')
        message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        if len(results)>0:
            pass
        else:  
            traded[isSymbol] = False

def takeTradeHedged(symbol,openingPrice, adder, adderQTY, modulus, shortX, strikeDateSymbol, qtyEXP, tradingExpiry ):
    global tokenList, tokenPair, callBuySymbols, putBuySymbols, hedgeList
    try:
        results = []
        isSymbol = 'is'+shortX+'TradedBuy'
        traded[isSymbol] = True
        # variableToBeUpdated = True
        CEStrikePrice = openingPrice + adderQTY*adder
        CEStrikePrice = modulus * round(CEStrikePrice / modulus)
        PEStrikePrice = openingPrice - adderQTY*adder
        PEStrikePrice = modulus * round(PEStrikePrice / modulus)
        CETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
        CETsymSymbolToken = CETsymSymbol['token'].values[0]
        lotSize = int(CETsymSymbol['lotSize'].values[0])
        CETsymSymbol       = CETsymSymbol['tsym'].values[0]
        PETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
        PETsymSymbolToken = PETsymSymbol['token'].values[0]
        PETsymSymbol = PETsymSymbol['tsym'].values[0]
        callBuySymbols.append(CETsymSymbol)
        putBuySymbols.append(PETsymSymbol)
        
        tokenList.append('NFO|'+str(CETsymSymbolToken))
        tokenList.append('NFO|'+str(PETsymSymbolToken))
        api.subscribe(['NFO|'+str(CETsymSymbolToken), 'NFO|'+str(PETsymSymbolToken)])
        hedgeList.append((str(CETsymSymbolToken), CETsymSymbol))
        hedgeList.append((str(PETsymSymbolToken), PETsymSymbol))
        
        logger.info(f'{CETsymSymbol, PETsymSymbol}')
        if strikeDateSymbol==str(datetime.now().date()) and tradingExpiry:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                orders = [
                    executor.submit(place_order, api, 'B', 'M', 'NFO', CETsymSymbol, (qtyEXP)*lotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSymbol,0, qtyEXP, "HEDGE "+shortX}'),
                    executor.submit(place_order, api, 'B', 'M', 'NFO', PETsymSymbol, (qtyEXP)*lotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSymbol,0, qtyEXP, "HEDGE "+shortX}'),
                ]
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                results = [order.result() for order in orders]
                client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {isSymbol: True, 'lastUpdatedTime': datetime.now()}})
                logger.info(f"Order List {results}")
                orderBookX = [('B', CETsymSymbol,qtyEXP*lotSize,'NFO',qtyEXP),('B',PETsymSymbol,qtyEXP*lotSize,'NFO',qtyEXP)]
        else:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                orders = [
                    executor.submit(place_order, api, 'B', 'M', 'NFO', CETsymSymbol, (quantities[shortX])*lotSize, 0, 'MKT', 0, None, 'DAY', f'{CETsymSymbol,quantities[shortX],0, "HEDGE "+shortX}'),
                    executor.submit(place_order, api, 'B', 'M', 'NFO', PETsymSymbol, (quantities[shortX])*lotSize, 0, 'MKT', 0, None, 'DAY', f'{PETsymSymbol,quantities[shortX],0, "HEDGE "+shortX}')
                ]
                # Wait for all orders to complete (this will not block)
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)
                results = [order.result() for order in orders]
                client['creds']['orderbook'].update_one({'uniqueIdentifier': mongoIdentifier}, {'$set': {isSymbol: True, 'lastUpdatedTime': datetime.now()}})
                orderBookX = [('B', CETsymSymbol,quantities[shortX]*lotSize,'NFO',quantities[shortX]),('B',PETsymSymbol,quantities[shortX]*lotSize,'NFO',quantities[shortX])]
                logger.info(f"Order List {results}")
        if shortX=='MID':
            midXOrders.append(orderBookX[0])
            midXOrders.append(orderBookX[1])
        elif shortX=='BNF':
            BNFOrders.append(orderBookX[0])
            BNFOrders.append(orderBookX[1])
        elif shortX=='NF':
            NFOrders.append(orderBookX[0])
            NFOrders.append(orderBookX[1])
        elif shortX=='FNF':
            FINNIFTYOrders.append(orderBookX[0])
            FINNIFTYOrders.append(orderBookX[1])
            
        traded[isSymbol] = True
        with open(f'{basedir}/tokenList.json', 'w') as f:
            json.dump(tokenList, f)
        with open(f'{basedir}/tokenPair.json', 'w') as f:
            json.dump(tokenPair, f)
        with open(f'{basedir}/hedgeList.json', 'w') as f:
                json.dump(hedgeList, f)    
        logger.info(f'{orderBookX}')
        return orderBookX
    except Exception as e:
        logger.error(f'{e}')
        message = f"🔴 🔴 | Error While Buying {symbol}| {e}"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        if len(results)>0:
            pass
        else:  
            traded[isSymbol] = False

def event_handler_quote_update(message):
    global SYMBOLDICT
    # global CETsymBNF, CETsymMIDCP, PETsymBNF, PETsymMIDCP, BNFOrders, midXOrders, bankexOrders, NFOrders
    global price
    global dfX
    
    key = message['e'] + '|' + message['tk']
    threads = []
    if key in SYMBOLDICT:
        symbol_info =  SYMBOLDICT[key]
        symbol_info.update(message)
        SYMBOLDICT[key] = symbol_info
    else:
        SYMBOLDICT[key] = message
    print(message)
    if ((datetime.now().hour == 10 and datetime.now().minute==15 and datetime.now().second >=58) or (datetime.now().hour == 10 and datetime.now().minute>=16)):
            if  message['tk']=='26009' and (not traded['isBNFTraded']):
                t = threading.Thread(target=takeTradeOld, args=('BANKNIFTY', float(SYMBOLDICT[key]['lp'])))
                threads.append(t)
                t.start()
    
    if ((datetime.now().hour == 9 and datetime.now().minute==15 and datetime.now().second >=58) or (datetime.now().hour == 9 and datetime.now().minute>=16)):
        
        if  message['tk']=='26074' and (not traded['isMIDTraded']):
            t = threading.Thread(target=takeTradeOld, args=('MIDCPNIFTY', float(SYMBOLDICT[key]['lp'])))
            threads.append(t)
            t.start()
        
        if  message['tk']=='26000' and (not traded['isNFTraded']):
            t = threading.Thread(target=takeTradeOld, args=('NIFTY', float(SYMBOLDICT[key]['lp'])))
            threads.append(t)
            t.start()     
        
        if  message['tk']=='12' and (not traded['isBANKEXTraded']):
            t = threading.Thread(target=takeTradeOld, args=("BANKEX", float(SYMBOLDICT[key]['lp'])))
            threads.append(t)
            t.start()
            
        if  message['tk']=='1' and (not traded['isSENSEXTraded']):
            t = threading.Thread(target=takeTradeOld, args=("SENSEX", float(SYMBOLDICT[key]['lp'])))
            threads.append(t)
            t.start()
        
        if  message['tk']=='26037' and (not traded['isFNFTraded']):
            t = threading.Thread(target=takeTradeOld, args=('FINNIFTY', float(SYMBOLDICT[key]['lp'])))
            threads.append(t)
            t.start()    
            
    if ((datetime.now().hour == 10 and datetime.now().minute==15 and datetime.now().second >=55) or (datetime.now().hour == 10 and datetime.now().minute>=16)):
        if message['tk'] in toTradeIndexForBuy:
            if message['tk']=='26009' and (not traded['isBNFTradedBuy']):
                t = threading.Thread(target=takeTradeHedged, args=('BANKNIFTY', float(SYMBOLDICT[key]['lp']), 100, 32, 500, "BNF", strikeDateBNF, quantities["BNFEXP"],True))
                threads.append(t)
                t.start()   
    
    if ((datetime.now().hour == 9 and datetime.now().minute==15 and datetime.now().second >=55) or (datetime.now().hour == 9 and datetime.now().minute>=16)):
        if message['tk'] in toTradeIndexForBuy:
            if message['tk']=='26074' and (not traded['isMIDTradedBuy']):
                t = threading.Thread(target=takeTradeHedged, args=('MIDCPNIFTY', float(SYMBOLDICT[key]['lp']), 25, 21, 100, "MID", strikeDateMID, quantities["MIDEXP"],True))
                threads.append(t)
                t.start()

            if message['tk']=='26000' and (not traded['isNFTradedBuy']):
                t = threading.Thread(target=takeTradeHedged, args=('NIFTY', float(SYMBOLDICT[key]['lp']), 50, 20, 500, "NF", strikeDateNIFTY, quantities["NFEXP"],True))
                threads.append(t)
                t.start()
                
            if message['tk']=='26037' and (not traded['isFNFTradedBuy']):
                t = threading.Thread(target=takeTradeHedged, args=('FINNIFTY', float(SYMBOLDICT[key]['lp']), 50, 21, 100, "FNF", strikeDateFINNIFTY, quantities["FNFEXP"],True))
                threads.append(t)
                t.start()
        
    if message['tk'] not in tick_data:
        tick_data[message['tk']] =  pd.DataFrame(columns=['timestamp', 'price'])
    tick_data[message['tk']].loc[len(tick_data[message['tk']])] = [pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')),float(SYMBOLDICT[key]['lp'])]
        
def open_callback():
    global socket_opened, tokenList
    socket_opened = True
    api.subscribe(tokenList)
    logger.info('app is connected')

def getBFO():
    try:
        req = Request(url='https://pidata.flattrade.in/scripmaster/json/bfoidx', headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
    except:
        message = f"🔴 🔴 | CANNOT UPDATE EXPIRY DATES"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()
        raise ValueError('Cannot update expiry dates')
    data_json = json.loads(webpage)
    data_json = data_json['data']
    df = pd.DataFrame(data_json)
    df = df.loc[((df['symbol']=="BANKEX") | (df['symbol']=="SENSEX") ) & (df['instrument']=='OPTIDX')]
    df.drop(['instrument'], axis=1, inplace=True)
    df.rename(columns={'exchange':'exch','symbol':'Symbol','tradingsymbol':'tsym','expiry':'Expiry','optiontype':'optt','strike':'StrikePrice','lotsize':"lotSize"}, inplace=True)
    df['Expiry'] = pd.to_datetime(df['Expiry'], infer_datetime_format=True)
    df['Expiry'] = df['Expiry'].apply(lambda x: str(x.date()))
    df['Expiry'] = pd.to_datetime(df['Expiry'], format=f'%Y-%m-%d').dt.strftime(f'%Y-%m-%d')
    df['StrikePrice'] = df['StrikePrice'].astype(float)
    df['StrikePrice'] = df['StrikePrice'].astype(int)
    if len(df)>100:
        pass
    else:
        zip_url = 'https://api.shoonya.com/BFO_symbols.txt.zip'
        r = requests.get(zip_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall('./Data/')
        df = pd.read_csv('./Data/BFO_symbols.txt')
        df = df.loc[((df['Symbol']=='BSXOPT') | (df['Symbol']=='BKXOPT') ) & (df['Instrument']=='OPTIDX')]
        try:
            df.drop(['Instrument', 'TickSize', 'Unnamed: 10'], axis=1, inplace=True)
        except:
            df.drop(['Instrument', 'TickSize'], axis=1, inplace=True)
        df.rename(columns={'exchange':'exch','Symbol':'Symbol','TradingSymbol':'tsym','OptionType':'optt', 'Token':'token','lotsize':"lotSize"}, inplace=True)
        df['Expiry'] = pd.to_datetime(df['Expiry'], infer_datetime_format=True)
        df['Expiry'] = df['Expiry'].apply(lambda x: str(x.date()))
        df['Expiry'] = pd.to_datetime(df['Expiry'], format=f'%Y-%m-%d').dt.strftime(f'%Y-%m-%d')
        df['StrikePrice'] = df['StrikePrice'].astype(float)
        # df['StrikePrice'] = df['StrikePrice']
        df['StrikePrice'] = df['StrikePrice'].astype(int)
        df['Symbol']= df['Symbol'].str.replace('BSXOPT',"SENSEX")
        df['Symbol']= df['Symbol'].str.replace('BKXOPT',"BANKEX")    
    return df

def expiry_helper(strikeDateMID, strikeDateBNF, strikeDateBANKEX, strikeDateSENSEX, strikeDateNIFTY, strikeDateFINNIFTY):
    try:
        req = Request(url='https://pidata.flattrade.in/scripmaster/json/nfoidx', headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
    except:
        message = f"🔴 🔴 | CANNOT UPDATE EXPIRY DATES"
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()
        raise ValueError('Cannot update expiry dates')
    data_json = json.loads(webpage)
    data_json = data_json['data']
    df = pd.DataFrame(data_json)
    df = df.loc[((df['symbol']=='BANKNIFTY') | (df['symbol']=='MIDCPNIFTY') | (df['symbol']=='NIFTY') | (df['symbol']=='FINNIFTY')) & (df['instrument']=='OPTIDX')]
    df.drop(['instrument'], axis=1, inplace=True)
    df.rename(columns={'exchange':'exch','symbol':'Symbol','tradingsymbol':'tsym','expiry':'Expiry','optiontype':'optt','strike':'StrikePrice','lotsize':"lotSize"}, inplace=True)
    df['Expiry'] = pd.to_datetime(df['Expiry'], infer_datetime_format=True)
    df['Expiry'] = df['Expiry'].apply(lambda x: str(x.date()))
    df['Expiry'] = pd.to_datetime(df['Expiry'], format=f'%Y-%m-%d').dt.strftime(f'%Y-%m-%d')
    df['StrikePrice'] = df['StrikePrice'].astype(float)
    df['StrikePrice'] = df['StrikePrice'].astype(int)
    # print(df.head(5))
    dfXX = getBFO()
    df = pd.concat([df,dfXX])
    df.to_csv(f'{basedir}/Data/NFO_symbols.csv', index=False)
    
    # '''check if expiry has to be bought first'''
    if strikeDateBNF == str(datetime.now().date()):
        toTradeIndexForBuy.append('26009')
    if strikeDateMID == str(datetime.now().date()):
        toTradeIndexForBuy.append('26074')
    if strikeDateNIFTY == str(datetime.now().date()):
        toTradeIndexForBuy.append('26000')
    if strikeDateFINNIFTY == str(datetime.now().date()):
        toTradeIndexForBuy.append('26037')
    
    # '''change old expiries'''
    if strikeDateBNF<str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=='BANKNIFTY')]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateBNF = expiries[0]
        cred['strikeDateBNF'] = strikeDateBNF
        # toTradeIndexForBuy.append('26009')
    
    if strikeDateMID<str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=='MIDCPNIFTY')]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateMID = expiries[0]
        cred['strikeDateMID'] = strikeDateMID
        # toTradeIndexForBuy.append('2'6074')
    
    if strikeDateNIFTY < str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=='NIFTY')]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateNIFTY = expiries[0]
        cred['strikeDateNIFTY'] = strikeDateNIFTY
        # toTradeIndexForBuy.append('26000')
    
    if strikeDateBANKEX<str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=="BANKEX")]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateBANKEX = expiries[0]
        cred['strikeDateBANKEX'] = strikeDateBANKEX
    
    if strikeDateSENSEX < str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=="SENSEX")]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateSENSEX = expiries[0]
        cred['strikeDateSENSEX'] = strikeDateSENSEX
    
    if strikeDateFINNIFTY < str(datetime.now().date()):
        BNFDF = df.loc[(df['Symbol']=='FINNIFTY')]
        expiries = sorted(BNFDF['Expiry'].unique())
        strikeDateFINNIFTY = expiries[0]
        cred['strikeDateFINNIFTY'] = strikeDateFINNIFTY
        # toTradeIndexForBuy.append('26037')
    
    updateMongo('creds', cred)
    return strikeDateMID, strikeDateBNF, strikeDateBANKEX, strikeDateSENSEX, strikeDateNIFTY, strikeDateFINNIFTY, df
    
def place_order(api, buy_or_sell, product_type, exchange, tradingsymbol, quantity, discloseqty, 
                price_type, price, trigger_price, retention, remarks):
    # return
    logger.info(f"Placing Order {buy_or_sell, product_type, exchange, tradingsymbol, quantity, discloseqty, price_type, price, trigger_price, retention, remarks}")
    # return api.place_order(buy_or_sell=buy_or_sell, product_type=product_type, exchange=exchange,
    #                     tradingsymbol=tradingsymbol, quantity=quantity, discloseqty=discloseqty,
    #                     price_type=price_type, price=price, trigger_price=trigger_price,
    #                     retention=retention, remarks=remarks)

def event_handler_socket_close():
    message = f"🔴 🔴 | SOCKET ERROR CLOSED!!! SOCKET ERROR CLOSED!!!"
    url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
    requests.get(url).json()

def event_handler_socket_error(error):
    message = f"🔴 🔴 | SOCKET ERROR!!! SOCKET ERROR {error}!!!"
    url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
    requests.get(url).json()
    # raise ValueError('Markets Closed')

def getSymbolForReBuy(tsym):
    _,Y = tsym.split('NIFTY')
    if 'C' in Y:
        side = 'CE'
    else:
        side = 'PE'
    if tsym.startswith('BANKNIFTY'):
        adderQTY = 30
        adder = 100
        modulus = 500
        token = '26009'
        symbol = 'BANKNIFTY'
        strikeDateSymbol = strikeDateBNF
    elif tsym.startswith('NIFTY'):
        adderQTY = 18
        adder = 50
        modulus = 500
        token = '26000'
        symbol = 'NIFTY'
        strikeDateSymbol = strikeDateNIFTY
        
    elif tsym.startswith('MIDCPNIFTY'):
        adderQTY = 19
        adder = 25
        modulus = 100
        token = '26074'
        symbol = 'MIDCPNIFTY'
        strikeDateSymbol = strikeDateMID
        
    elif tsym.startswith('FINNIFTY'):
        adderQTY = 19
        adder = 50
        modulus = 100
        token = '26037'
        symbol = 'FINNIFTY'
        strikeDateSymbol = strikeDateFINNIFTY
    
    CEStrikePrice = tick_data[token].iloc[-1]['price'] + adderQTY*adder
    CEStrikePrice = modulus * round(CEStrikePrice / modulus)
    PEStrikePrice = tick_data[token].iloc[-1]['price'] - adderQTY*adder
    PEStrikePrice = modulus * round(PEStrikePrice / modulus)
    CETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(CEStrikePrice)) & (dfX['optt']=='CE')]
    CETsymSymbolToken = CETsymSymbol['token'].values[0]
    lotSize = int(CETsymSymbol['lotSize'].values[0])
    CETsymSymbol       = CETsymSymbol['tsym'].values[0]
    PETsymSymbol      = dfX[(dfX['Symbol']==symbol) & (dfX['Expiry']==strikeDateSymbol) & (dfX['StrikePrice']==int(PEStrikePrice)) & (dfX['optt']=='PE')]
    PETsymSymbolToken = PETsymSymbol['token'].values[0]
    PETsymSymbol = PETsymSymbol['tsym'].values[0]
    # callBuySymbols.append(CETsymSymbol)
    # putBuySymbols.append(PETsymSymbol)
    if side=='CE':
        return CETsymSymbol, CETsymSymbolToken, lotSize
    else:
        return PETsymSymbol, PETsymSymbolToken, lotSize
    
def calculatePNL(buyAmount, sellAmount, buyqty,sellQty, cmp):
    value = sellAmount - buyAmount
    if buyqty>sellQty and sellQty==0:
        value = (cmp - buyAmount)*buyqty
    elif sellQty>buyqty and buyqty==0:
        value = (sellAmount - cmp)*sellQty
    elif buyqty>sellQty:
        value = (sellAmount-buyAmount)*(buyqty-sellQty)
        value += (cmp - buyAmount)*(buyqty-sellQty)
    elif sellQty>buyqty:
        value = (sellAmount-buyAmount)*(sellQty-buyqty)
        value += (sellAmount - cmp)*(sellQty-buyqty)
    elif buyqty==sellQty:
        value = (sellAmount-buyAmount)*buyqty
    return value
    
isCompleted = False
socket_opened = False
if api.get_holdings()==None:
    message = f"🔴 🔴| ALERT ALERT ALERT | FT Login Error | Failed Login"
    url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
    requests.get(url).json()
    ret = reauth()
else:
    message = f"🟢 🟢 | WE ARE A GO | {strikeDateMID, strikeDateBNF, strikeDateBANKEX, strikeDateSENSEX, strikeDateNIFTY, strikeDateFINNIFTY}|"
    url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
    requests.get(url).json()

global callBuySymbols,putBuySymbols, toTradeIndexForBuy
toTradeIndexForBuy = []
callBuySymbols = []
putBuySymbols = []
hedgeList = []
tokenList = []

statusDay = ''
strikeDateMID, strikeDateBNF, strikeDateBANKEX, strikeDateSENSEX, strikeDateNIFTY,strikeDateFINNIFTY, dfX = expiry_helper(strikeDateMID, strikeDateBNF, strikeDateBANKEX, strikeDateSENSEX, strikeDateNIFTY, strikeDateFINNIFTY)
requests.get(url).json()
'''sensex 1, midcpnifty 26074, bankex 12, banknifty 26009, nifty 26000, finnifty 26037'''

positions = api.get_positions()
priceCollector = {}

def makeCorrectionsInPriceCollector(positions):
    lotSize = {
        'MIDCPNIFTY':50,
        'NIFTY':25,
        'FINNIFTY':25,
        'BANKNIFTY':15,
        'SENSEX':10
    }
    for position in positions:
        totalBuyQty = int(position['daybuyqty']) + int(position['cfbuyqty'])
        totalSellQty = int(position['daysellqty']) + int(position['cfsellqty'])
        if position['tsym'].startswith('BANKNIFTY'):
            lotQTY=lotSize['BANKNIFTY']
        elif position['tsym'].startswith('NIFTY'):
            lotQTY=lotSize['NIFTY']
        elif position['tsym'].startswith('FINNIFTY'):
            lotQTY=lotSize['FINNIFTY']
        elif position['tsym'].startswith('MIDCPNIFTY'):
            lotQTY=lotSize['MIDCPNIFTY']
        elif position['tsym'].startswith('SENSEX'):
            lotQTY=lotSize['SENSEX']
        
        if totalBuyQty>totalSellQty:
            lotQTY = int(totalBuyQty/lotQTY)
        else:
            lotQTY = int(totalSellQty/lotQTY)
        
        if totalBuyQty-totalSellQty==0:
            SL = True
            HF = True
        elif totalSellQty-totalBuyQty==totalSellQty:
            '''sell Side'''
            SL = False
            HF = False
        elif totalBuyQty-totalSellQty==totalBuyQty:
            '''buy Side'''
            SL = False
            HF = False
            hedged = True
        else:
            SL = True
            HF = True
        # if position
        priceCollector[position['tsym']] = {"exchange": position['exch'], "buyPrice": float(position['totbuyavgprc']), "buyQty": totalBuyQty, "sellPrice": float(position['totsellavgprc']), "sellQty": totalSellQty, "lotQty": lotQTY, "SL": SL, "halfProfit": HF}
        if position['daybuyqty']>position['daysellqty'] or position['cfbuyqty']> position['cfsellqty'] and totalBuyQty!=totalSellQty:
            if (position['token'], position['tsym']) not in hedgeList:
                hedgeList.append((position['token'], position['tsym']))
                
        if position['exch']+"|"+position['token'] not in tokenList:
            tokenList.append(position['exch']+"|"+position['token'])
        
    # print(len(priceCollector), priceCollector)

global FINNIFTYOrders, midXOrders, BNFOrders, NFOrders, BANKEXOrders, SENSEXOrders
FINNIFTYOrders, midXOrders, BNFOrders, NFOrders, BANKEXOrders, SENSEXOrders = [], [], [], [], [], []
makeCorrectionsInPriceCollector(positions)

Indexes = ['MIDCPNIFTY', 'BANKNIFTY', 'NIFTY', 'FINNIFTY']
for hedges in hedgeList:
    token = hedges[0]
    tsym = hedges[1]
    if tsym.startswith('BANKNIFTY'):
        if 'BANKNIFTY' in Indexes:
            Indexes.remove('BANKNIFTY')
    elif tsym.startswith('NIFTY'):
        if 'NIFTY' in Indexes:
            Indexes.remove('NIFTY')
    elif tsym.startswith('FINNIFTY'):
        if 'FINNIFTY' in Indexes:
            Indexes.remove('FINNIFTY')
    elif tsym.startswith('MIDCPNIFTY'):
        if 'MIDCPNIFTY' in Indexes:
            Indexes.remove('MIDCPNIFTY')
        # Indexes.remove('MIDCPNIFTY')

for index in Indexes:
    if index=='BANKNIFTY':
        if '26009' not in toTradeIndexForBuy:
            toTradeIndexForBuy.append('26009')
    elif index=='NIFTY':
        if '26000' not in toTradeIndexForBuy:
            toTradeIndexForBuy.append('26000')
    elif index=='FINNIFTY':
        if '26037' not in toTradeIndexForBuy:
            toTradeIndexForBuy.append('26037')
    elif index=='MIDCPNIFTY':
        if '26074' not in toTradeIndexForBuy:
            toTradeIndexForBuy.append('26074')

logger.info(f'{toTradeIndexForBuy =}')
message = f"Indexes to buy today: {toTradeIndexForBuy}"
url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"

if 'NSE|26074' not in tokenList:
    tokenList.append('NSE|26074')
if 'NSE|26009' not in tokenList:
    tokenList.append('NSE|26009')
if 'NSE|26037' not in tokenList:
    tokenList.append('NSE|26037')

if strikeDateNIFTY==str(datetime.now().date()):
    tokenList.append('NSE|26000')
elif strikeDateSENSEX==str(datetime.now().date()):
    tokenList.append('BSE|1')
else:
    tokenList.append('NSE|26000')
for traded in client['creds']['orderbook'].find({'uniqueIdentifier':mongoIdentifier}):
    pass

logger.info(f'PriceCollector: {len(priceCollector),priceCollector}')
logger.info(f'Hedges: {len(hedgeList),hedgeList}')
logger.info(f'TokenList: {len(tokenList),tokenList}')



if len(priceCollector)!=0:
    print("Price Collector is not empty")
    with open(f'{basedir}/tokenPair.json') as f:
        tokenPair = json.load(f)
    
ret = api.start_websocket(order_update_callback=event_handler_order_update,socket_open_callback=open_callback, subscribe_callback=event_handler_quote_update, socket_close_callback=event_handler_socket_close, socket_error_callback=event_handler_socket_error)
LTP = {}
subscribe_flag = False
subscribe_list = []
unsubscribe_list = []

if int(cred['month'])!=datetime.now().month:
    monthlyP = cred['MonthlyProfit']
    if monthlyP>0:
        message = f'🟢 🟢 || {monthlyP} was your previous Month PNL'
    else:
        message = f'🔴 🔴 || {monthlyP} was your previous Month PNL'
    cred['MonthlyProfit'] = 0
    cred['month'] = datetime.now().month
    url = f"https://api.telegram.org/bot{TGToken2}/sendMessage?chat_id={chatID}&text={str(message)}"
    requests.get(url).json()
    updateMongo('creds', cred)

quantities = {
    "MID":8,
    "BNF":4,
    "NF":8,
    "FNF":4,
    "SENSEX":2,
    "MIDEXP":2,
    "BNFEXP":2,
    "NFEXP":2,
    "FNFEXP":2,
    "SENSEXEXP":2}

expiryNotTradedToday = True

tradingExpiry = True
open_callback()
threads = []
prevDateTime = datetime.now().hour*60*60 + datetime.now().minute*60
lastBusDay = datetime.today()
lastBusDay = lastBusDay.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
CheckedDiscrepancyInPriceCollector = False
logger.info(f'{traded=}')
while True:
    time.sleep(0.1)
    try:
        #Send TG a starting message
        if datetime.now().hour ==9 and datetime.now().minute==15 and datetime.now().second==30:
            message = f'🟢 🟢 || We are still running!!'
            url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
            requests.get(url).json()
        
        #Close the program at 3:27:57
        if datetime.now().hour == 15 and datetime.now().minute>=27 and datetime.now().second>=57 and (not isCompleted):
            logger.info(f'Closing the program')
            logger.info(f'{priceCollector}')
            with concurrent.futures.ThreadPoolExecutor() as executor:
                finList = []
                for symbol in priceCollector.keys():
                    if priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty']>0:
                        finList.append([symbol, priceCollector[symbol]['exchange'], priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty']])
                orders = [ executor.submit(place_order, api, 'B', 'M', exch, sym, qty, 0, 'MKT', 0, None, 'DAY', f'{sym, datetime.now()}') for sym, exch,qty in finList]
                # Wait for all orders to complete (this will not block)
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)

                results = [order.result() for order in orders]
            isCompleted = True
            time.sleep(10)
            logger.info(f'Closing the program 2')
            logger.info(f'{priceCollector}')
            makeCorrectionsInPriceCollector(api.get_positions())
            with concurrent.futures.ThreadPoolExecutor() as executor:
                finList = []
                for symbol in priceCollector.keys():
                    if priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty']>0:
                        finList.append([symbol, priceCollector[symbol]['exchange'], priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty']])
                orders = [ executor.submit(place_order, api, 'B', 'M', exch, sym, qty, 0, 'MKT', 0, None, 'DAY', f'{sym, datetime.now()}') for sym, exch,qty in finList]
                # Wait for all orders to complete (this will not block)
                concurrent.futures.wait(orders, return_when=concurrent.futures.ALL_COMPLETED)

                results = [order.result() for order in orders]
            
            
            symbolsToDeleteFromPriceCollector = []
            pnl = 0
            for symbol in priceCollector.keys():
                if priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty'] ==0:
                    pnl += calculatePNL(priceCollector[symbol]['buyPrice'], priceCollector[symbol]['sellPrice'], priceCollector[symbol]['buyQty'], priceCollector[symbol]['sellQty'], 0.05)
                    symbolsToDeleteFromPriceCollector.append(symbol)
                if symbol.startswith('MIDCPNIFTY') and strikeDateMID==str(datetime.now().date()):
                    pnl += calculatePNL(priceCollector[symbol]['buyPrice'], priceCollector[symbol]['sellPrice'], priceCollector[symbol]['buyQty'], priceCollector[symbol]['sellQty'], 0.05)
                if symbol.startswith('BANKNIFTY') and strikeDateBNF==str(datetime.now().date()):
                    pnl += calculatePNL(priceCollector[symbol]['buyPrice'], priceCollector[symbol]['sellPrice'], priceCollector[symbol]['buyQty'], priceCollector[symbol]['sellQty'], 0.05)
                if symbol.startswith('NIFTY') and strikeDateNIFTY==str(datetime.now().date()):
                    pnl += calculatePNL(priceCollector[symbol]['buyPrice'], priceCollector[symbol]['sellPrice'], priceCollector[symbol]['buyQty'], priceCollector[symbol]['sellQty'], 0.05)
                    
            for symbol in symbolsToDeleteFromPriceCollector:
                del priceCollector[symbol]
            
            with open(f'{basedir}/priceCollector.json', 'w') as f:
                json.dump(priceCollector, f)
            
            with open(f'{basedir}/tokenList.json', 'w') as f:
                json.dump([], f)
            with open(f'{basedir}/hedgeList.json', 'w') as f:
                json.dump([], f)
            
            with open(f'{basedir}/tokenPair.json', 'w') as f:
                json.dump([], f)
            traded['isMIDTraded'] = False
            traded['isBNFTraded'] = False
            traded['isNFTraded'] = False
            traded['isFNFTraded'] = False
            traded['isSENSEXTraded'] = False
            traded['isBANKEXTraded'] = False
            traded['isMIDTradedBuy'] = False
            traded['isBNFTradedBuy'] = False
            traded['isNFTradedBuy'] = False
            traded['isFNFTradedBuy'] = False
            client['creds']['orderbook'].update_one({'uniqueIdentifier': 'utsavFlattradeMainAccount'}, {'$set': traded})
            day_m2m = pnl
            totalP = cred['MonthlyProfit']+day_m2m
            if day_m2m>0:
                message = f'🟢 🟢 || {day_m2m} is your Daily MTM || {totalP} is Monthly M2M'
            else:
                message = f'🔴 🔴 || {day_m2m} is your Daily MTM || {totalP} is Monthly M2M'
            # message = f'{day_m2m} is your Daily MTM'
            url = f"https://api.telegram.org/bot{TGToken2}/sendMessage?chat_id={chatID}&text={str(message)}"
            requests.get(url).json()
            cred['MonthlyProfit']+=day_m2m
            updateMongo('creds', cred)
            # with open(f'{basedir}/cred.yml', 'w') as f:
            #         yaml.dump(cred, f)
            break      
        
        #Check Inaccuracies in PriceCollector
        if datetime.now().hour == 9 and datetime.now().minute==16 and datetime.now().second == 0 and (not CheckedDiscrepancyInPriceCollector):
            CheckedDiscrepancyInPriceCollector = True
            positions = api.get_positions()
            makeCorrectionsInPriceCollector(positions)
            missingOrders = []
            for valx in NFOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            for valx in BNFOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            for valx in midXOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            for valx in FINNIFTYOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            for valx in BANKEXOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            for valx in SENSEXOrders:
                if valx[1] not in priceCollector.keys():
                    missingOrders.append(valx)
            logger.info(f'{missingOrders}')
            for order in missingOrders:
                place_order(api, order[0], 'M', order[3], order[1], order[2], 0, 'MKT', 0, None, 'DAY', f'{order[1], order[4]}')
                
        #SL & HF Code
        if ((datetime.now().hour == 9 and datetime.now().minute >=17) or (datetime.now().hour>9))  and (prevDateTime!=datetime.now().hour*60*60 + datetime.now().minute*60) and datetime.now().second==0:
            logger.info("-----"*20)
            print("-----"*20)
            prevDateTime = datetime.now().hour*60*60 + datetime.now().minute*60
            # value = 0
            #SL & HF Conditions for sellSide
            for symbols in tokenPair:
                try:
                    token1 = symbols[0][0]
                    symbolX1 = symbols[0][1]
                    token2 = symbols[1][0]
                    symbolX2 = symbols[1][1]
                    
                    symbol1SellPrice = priceCollector[symbolX1]['sellPrice']
                    symbol1Exchange = priceCollector[symbolX1]['exchange']
                    symbol2SellPrice = priceCollector[symbolX2]['sellPrice']
                    symbol2Exchange = priceCollector[symbolX2]['exchange']
                    
                    
                    if (token1 in tick_data) and (token2 in tick_data):
                        try:
                            if ((tick_data[token1].iloc[-1]['timestamp'] +timedelta(minutes=1)) < datetime.now()) or ((tick_data[token2].iloc[-1]['timestamp'] +timedelta(minutes=1)) < datetime.now()):
                                logger.error(f'{tick_data[token1].iloc[-1]["timestamp"], tick_data[token2].iloc[-1]["timestamp"]}')
                                raise ValueError('Data is not updated')
                            
                            tick_data[token1].index = pd.to_datetime(tick_data[token1]['timestamp'])
                            resampled_1mS1 = tick_data[token1].resample('1min').agg({'price': 'ohlc'})
                            resampled_1mS1.columns = ['open', 'high', 'low', 'close']
                            closeS1 = resampled_1mS1[resampled_1mS1.index == (pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))-timedelta(minutes=1)).replace(second=0)]['close'].values[0]
                            
                            tick_data[token2].index = pd.to_datetime(tick_data[token2]['timestamp'])
                            resampled_1mS2 = tick_data[token2].resample('1min').agg({'price': 'ohlc'})
                            resampled_1mS2.columns = ['open', 'high', 'low', 'close']
                            closeS2 = resampled_1mS2[resampled_1mS2.index == (pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))-timedelta(minutes=1)).replace(second=0)]['close'].values[0]
                            # logger.info(f'{closeS1, closeS2}')
                        except Exception as e:
                            logger.info(f"Into Exception, fetching using timePriceSeries {e}")
                            resampled_1mS1 = api.get_time_price_series(exchange=symbol1Exchange, token=str(token1), starttime=lastBusDay, interval=1)
                            closeS1 = float(resampled_1mS1[0]['intc'])
                            resampled_1mS2 = api.get_time_price_series(exchange=symbol2Exchange, token=str(token2), starttime=lastBusDay, interval=1)
                            closeS2 = float(resampled_1mS2[0]['intc'])
                            # logger.info(f'{closeS1, closeS2}')
                        
                        CECurrentPL = symbol1SellPrice - closeS1
                        PECurrentPL = symbol2SellPrice - closeS2
                        
                        if symbolX1.startswith('MIDCPNIFTY'):
                            HFFactor = 2
                            SLFactor = 2
                        elif symbolX1.startswith('BANKNIFTY'):
                            HFFactor = 3
                            SLFactor = 5
                        elif symbolX1.startswith('NIFTY'):
                            HFFactor = 3
                            SLFactor = 1.5
                        elif symbolX1.startswith("SENSEX"):
                            HFFactor = 3
                            SLFactor = 1.5
                        elif symbolX1.startswith('FINNIFTY'):
                            HFFactor = 1
                            SLFactor = 2
                        
                        logger.info(f'{symbolX1, closeS1, symbolX2,closeS2,float(CECurrentPL+PECurrentPL),"HF: ",float(symbol1SellPrice + symbol2SellPrice)/HFFactor, "SL: ",float((symbol1SellPrice + symbol2SellPrice)/SLFactor)}')
                        print(f'{symbolX1, closeS1, symbolX2,closeS2,float(CECurrentPL+PECurrentPL),"HF: ",float(symbol1SellPrice + symbol2SellPrice)/HFFactor, "SL: ",float((symbol1SellPrice + symbol2SellPrice)/SLFactor)}')
                        
                        if float(CECurrentPL+PECurrentPL) >= float((symbol1SellPrice + symbol2SellPrice)/HFFactor) and not priceCollector[symbolX1]['halfProfit'] and not priceCollector[symbolX2]['halfProfit']:
                            '''sell at half profit half qty of these 2 symbols'''
                            lotSize = priceCollector[symbolX1]['lotQty']
                            qtyA = ((priceCollector[symbolX1]['sellQty']- priceCollector[symbolX1]['buyQty'])/lotSize)*(int(lotSize/2))
                            qtyB = ((priceCollector[symbolX2]['sellQty']- priceCollector[symbolX2]['buyQty'])/lotSize)*(int(lotSize/2))
                            
                            place_order(api, 'B', 'M', symbol1Exchange, symbolX1, int(qtyA), 0, 'MKT', 0, None, 'DAY', f'{symbolX1, lotSize}')
                            place_order(api, 'B', 'M', symbol2Exchange, symbolX2, int(qtyB), 0, 'MKT', 0, None, 'DAY', f'{symbolX2, lotSize}')
                            priceCollector[symbolX1]['halfProfit'] = True
                            priceCollector[symbolX2]['halfProfit'] = True
                            print("ORDER PLACED!!!!!!!!! Half Profit")
                            print(float((symbol1SellPrice + symbol2SellPrice)/HFFactor),float(closeS1+closeS2))
                            logger.info(f'{"HALF PROFIT:-",symbolX1, qtyA, symbolX2, qtyB,(symbol1SellPrice + symbol2SellPrice)/HFFactor,float(closeS1+closeS2)}')
                            with open(f'{basedir}/priceCollector.json', 'w') as f:
                                json.dump(priceCollector, f)
                            
                        
                        if float(CECurrentPL+PECurrentPL) <= -1*float((symbol1SellPrice + symbol2SellPrice)/SLFactor) and not priceCollector[symbolX1]['SL'] and not priceCollector[symbolX2]['SL']:
                            '''sell all the qty'''
                            qtyA = ((priceCollector[symbolX1]['sellQty']- priceCollector[symbolX1]['buyQty']))
                            qtyB = ((priceCollector[symbolX2]['sellQty']- priceCollector[symbolX2]['buyQty']))
                            logger.info(f'{"STOPLOSS HIT:-",CECurrentPL, PECurrentPL}')
                            print(f'{"STOPLOSS HIT:-",CECurrentPL, PECurrentPL}')
                            if CECurrentPL<0:
                                place_order(api, 'B', 'M', symbol1Exchange, symbolX1, int(qtyA), 0, 'MKT', 0, None, 'DAY', f'{symbolX1, qtyA}')
                                priceCollector[symbolX1]['SL'] = True
                                priceCollector[symbolX1]['halfProfit'] = True
                                logger.info(f'{"STOPLOSS HIT:-", symbolX1, CECurrentPL}')
                            if PECurrentPL<0:
                                place_order(api, 'B', 'M', symbol2Exchange, symbolX2, int(qtyB), 0, 'MKT', 0, None, 'DAY', f'{symbolX2, qtyB}')
                                priceCollector[symbolX2]['SL'] = True
                                priceCollector[symbolX2]['halfProfit'] = True
                                logger.info(f'{"STOPLOSS HIT:-", symbolX2, PECurrentPL}')
                                
                            with open(f'{basedir}/priceCollector.json', 'w') as f:
                                json.dump(priceCollector, f)
                except Exception as e:
                    print(e)
                    logger.error(f'{e}')
            
            #Sell Hedge at 2x and buy another
            for symbols in hedgeList:
                try:
                    token1 = symbols[0]
                    symbolX1 = symbols[1]
                    symbol1buyPrice = priceCollector[symbolX1]['buyPrice']
                    symbol1Exchange = priceCollector[symbolX1]['exchange']
                    if (token1 in tick_data):
                        try:
                            if ((tick_data[token1].iloc[-1]['timestamp'] +timedelta(minutes=1)) < datetime.now()):
                                logger.error(f'{tick_data[token1].iloc[-1]["timestamp"]}')
                                raise ValueError('Data is not updated')
                            tick_data[token1].index = pd.to_datetime(tick_data[token1]['timestamp'])
                            resampled_1mS1 = tick_data[token1].resample('1min').agg({'price': 'ohlc'})
                            resampled_1mS1.columns = ['open', 'high', 'low', 'close']
                            closeS1 = resampled_1mS1[resampled_1mS1.index == (pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))-timedelta(minutes=1)).replace(second=0)]['close'].values[0]
                        except Exception as e:
                            logger.info(f"Into Exception, fetching using timePriceSeries {e}")
                            resampled_1mS1 = api.get_time_price_series(exchange=symbol1Exchange, token=str(token1), starttime=lastBusDay, interval=1)
                            closeS1 = float(resampled_1mS1[0]['intc'])
                    else:
                        continue
                    CECurrentPL = closeS1 - symbol1buyPrice
                    logger.info(f'{symbolX1, closeS1, float(CECurrentPL)}')
                    qtySell = abs(priceCollector[symbolX1]['sellQty']- priceCollector[symbolX1]['buyQty'])
                    if closeS1>2*symbol1buyPrice and qtySell>0:
                        qtyBuy = priceCollector[symbolX1]['buyQty']
                        symb, token, lotSize = getSymbolForReBuy(symbolX1)
                        lotSize = int(qtyBuy/lotSize)
                        place_order(api, 'B', 'M', symbol1Exchange, symb, qtyBuy, 0, 'MKT', 0, None, 'DAY', f'{symb, lotSize}')
                        tokenList.append('NFO|'+str(symb))
                        api.subscribe(['NFO|'+str(symb)])
                        hedgeList.append((str(token), symb))
                        place_order(api, 'S', 'M', symbol1Exchange, symbolX1, qtySell, 0, 'MKT', 0, None, 'DAY', f'{symbolX1, lotSize}')
                        hedgeList.remove((token1, symbolX1))
                        
                except Exception as e:
                    logger.error(f'{e}')                       
                
        #Close after 3:30 if missed earlier
        if (datetime.now().hour == 15 and datetime.now().minute>=30) or datetime.now().hour>15:
                symbolsToDeleteFromPriceCollector = []
                for symbol in priceCollector.keys():
                    if priceCollector[symbol]['sellQty']- priceCollector[symbol]['buyQty'] ==0:
                        symbolsToDeleteFromPriceCollector.append(symbol)
                
                for symbol in symbolsToDeleteFromPriceCollector:
                    del priceCollector[symbol]
                
                with open(f'{basedir}/priceCollector.json', 'w') as f:
                    json.dump(priceCollector, f)
                
                with open(f'{basedir}/tokenList.json', 'w') as f:
                    json.dump([], f)
                with open(f'{basedir}/hedgeList.json', 'w') as f:
                    json.dump([], f)
                
                with open(f'{basedir}/tokenPair.json', 'w') as f:
                    json.dump([], f)
                traded['isMIDTraded'] = False
                traded['isBNFTraded'] = False
                traded['isNFTraded'] = False
                traded['isFNFTraded'] = False
                traded['isSENSEXTraded'] = False
                traded['isBANKEXTraded'] = False
                traded['isMIDTradedBuy'] = False
                traded['isBNFTradedBuy'] = False
                traded['isNFTradedBuy'] = False
                traded['isFNFTradedBuy'] = False
                client['creds']['orderbook'].update_one({'uniqueIdentifier': 'utsavFlattradeMainAccount'}, {'$set': traded})
                
                ret = api.get_positions()
                mtm = 0
                pnl = 0
                day_m2m = 0
                try:
                    for i in ret:
                        mtm += float(i['urmtom'])
                        pnl += float(i['rpnl'])
                        day_m2m = mtm + pnl
                except:
                    pass
                
                totalP = cred['MonthlyProfit']+day_m2m
                if day_m2m>0:
                    message = f'🟢 🟢 || {day_m2m} is your Daily MTM || {totalP} is Monthly M2M'
                else:
                    message = f'🔴 🔴 || {day_m2m} is your Daily MTM || {totalP} is Monthly M2M'
                # message = f'{day_m2m} is your Daily MTM'
                url = f"https://api.telegram.org/bot{TGToken2}/sendMessage?chat_id={chatID}&text={str(message)}"
                requests.get(url).json()
                
                # cred['user_token'] = ''
                cred['MonthlyProfit']+=day_m2m
                updateMongo('creds', cred)
                break
            # if datetime.now().hour == 15 and datetime.now().minute>=30:
    
    except Exception as e:
        message = f"🔴 🔴 | EXIT NOWWWWW!!!!!!!!!!!!!!!!! | FT Order Error| | {str(e)}"
        # logger.info(f'{midXOrders}, {BNFOrders}, {NFOrders}')
        url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
        requests.get(url).json()
        break

time.sleep(50)
# except Exception as e:
#     message = f"🔴 🔴 | BIG ERROR SOMETHING FUCKED UP | {e}"
#     url = f"https://api.telegram.org/bot{TGToken1}/sendMessage?chat_id={chatID}&text={str(message)}"
#     requests.get(url).json()