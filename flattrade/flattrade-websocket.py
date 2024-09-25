import asyncio
import websockets
import json
import logging
import hashlib
# from pya3 import *
from urllib.parse import parse_qs,urlparse
from urllib.request import Request, urlopen
import pyotp

""" Using the NorenRestApi latest package (NorenRestApiPy is class name, although there is a separate package with the same name NorenRestApiPy - older version, Don't get confused, we don't want NorenRestApiPy old package, we want NorenRestApi)
This package 'NorenRestApi' has to be installed without it's dependencies, otherwise it will not work, So we have added pip install --no-deps NorenRestApi in install-all.bat file
DO NOT CHANGE NorenRestApiPy to NorenRestApi """
from NorenRestApiPy.NorenApi import NorenApi
import requests
import time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import yaml

logging.basicConfig(level=logging.INFO)

# Flag to tell us if the websocket is open
socket_opened = False
basedir = '.'

with open(f'{basedir}/cred.yml') as f:
    impo = yaml.load(f, Loader=yaml.FullLoader)
mongoURI = impo['mongoURI']
mongoIdentifier = impo['mongoIdentifierNavya']
client = MongoClient(mongoURI , server_api=ServerApi('1'))
for cred in client['creds']['flattrade'].find({'uniqueIdentifier':mongoIdentifier}):
    pass



# Initialize the API object with required arguments
try:
    api = NorenApi(
        host="https://piconnect.flattrade.in/PiConnectTP/",
        websocket="wss://piconnect.flattrade.in/PiConnectWSTp/",
        eodhost="https://web.flattrade.in/chartApi/getdata/",
    )
except TypeError:
    # If 'eodhost' is not accepted, try without it
    api = NorenApi(
        host="https://piconnect.flattrade.in/PiConnectTP/",
        websocket="wss://piconnect.flattrade.in/PiConnectWSTp/",
    )


# Event handlers
def event_handler_order_update(message):
    print("order event: " + str(message))


def event_handler_quote_update(message):
    # print(f"quote event: {time.strftime('%d-%m-%Y %H:%M:%S')} {message}")
    # logging.info(f"Quote update received: {message}")
    asyncio.run_coroutine_threadsafe(quote_queue.put(message), loop)


async def get_credentials_and_security_ids():
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            None, lambda: requests.get("http://localhost:3000/flattrade-websocket-data")
        )
        response.raise_for_status()
        data = response.json()
        usersession = data.get("usersession", "")
        userid = data.get("userid", "")

        if usersession and userid:
            logging.info("Valid data retrieved successfully")
            return usersession, userid
        else:
            logging.info("Waiting for valid data...")
            return None, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to retrieve data: {e}")
        return None, None


# async def wait_for_data():
#     while True:
#         usersession, userid = (
#             await get_credentials_and_security_ids()
#         )
#         if usersession and userid:
#             return usersession, userid
#         await asyncio.sleep(5)  

def updateMongo(value, updation):
    if value=='creds':
        client['creds']['flattrade'].update_one({'uniqueIdentifier':mongoIdentifier},{'$set':updation})


userid = cred['user']
password = cred['pwd']
API_KEY = cred['apikey']
API_SECRET = cred['apisecret']
totp_key = cred['totp_key']
    


def reauth():
    global api
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
    print(usertoken)
    updateMongo('creds', cred)
    return ret

async def setup_api_connection(cred):
    global api
    ret = reauth()
    # ret = api.set_session(userid= cred['user_id'], password = cred['pwd'], usertoken= cred['user_token'])
    
    # Set up the session
    
    # ret = api.set_session(userid=userid, password="", usertoken=usersession)

    if ret is not None:
        # Start the websocket
        ret = api.start_websocket(
            order_update_callback=event_handler_order_update,
            subscribe_callback=event_handler_quote_update,
            socket_open_callback=open_callback,  # No parameters needed
        )
        print(ret)
    else:
        raise Exception("Failed to set up API session")


def open_callback():
    global socket_opened
    socket_opened = True
    print("app is connected")

    # Optionally, you can log or perform other actions here
    # without subscribing to hardcoded symbols


quote_queue = asyncio.Queue()


async def websocket_server(websocket, path):
    try:
        # Create a task to continuously send quote updates to the client
        send_task = asyncio.create_task(send_quote_updates(websocket))

        async for message in websocket:
            await handle_websocket_message(websocket, message)
    except websockets.exceptions.ConnectionClosed:
        print("Connection closed")
    finally:
        # Cancel the send task when the connection is closed
        send_task.cancel()


async def send_quote_updates(websocket):
    while True:
        try:
            quote = await quote_queue.get()
            await websocket.send(json.dumps(quote))
        except Exception as e:
            logging.error(f"Error sending quote update: {e}")
            # If there's an error, wait a bit before trying again
            await asyncio.sleep(1)


async def handle_websocket_message(websocket, message):
    data = json.loads(message)
    if "action" in data:
        if data["action"] == "unsubscribe":
            for symbol in data["symbols"]:
                api.unsubscribe([symbol])
                print(f"\nUnsubscribed from {symbol}")
                # logging.info(f"Unsubscribed from {symbol}")
        elif data["action"] == "subscribe":
            for symbol in data["symbols"]:
                api.subscribe([symbol])
                print(f"\nSubscribed to {symbol}")
                # logging.info(f"Subscribed to {symbol}")

            # Add a small delay after subscribing
            await asyncio.sleep(0.1)

            # Check for any pending quote updates
            while not quote_queue.empty():
                quote = await quote_queue.get()
                await websocket.send(json.dumps(quote))
    else:
        # Handle the existing credential update logic
        global usersession, userid
        usersession = data.get("usersession", "")
        userid = data.get("userid", "")
        print(
            f"Updated credentials and security IDs: {usersession[:5]}...{usersession[-5:]}, {userid[:2]}....{userid[-2:]}"
        )


async def main():
    global loop
    loop = asyncio.get_running_loop()

    try:
        # Wait for valid credentials and security IDs
        logging.info("Waiting for valid data...")
        # usersession, userid= (
        #     await wait_for_data()
        # )
        # logging.info(
        #     f"Using usersession: {usersession[:5]}...{usersession[-5:]}, userid: {userid[:2]}....{userid[-2:]}"
        # )

        # Set up API connection
        await setup_api_connection(cred)

        # Set up WebSocket server
        server = await websockets.serve(websocket_server, "localhost", 8765)
        await server.wait_closed()

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
