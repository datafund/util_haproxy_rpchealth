import logging
from flask import Flask, request
from flask_caching import Cache
from requests.exceptions import SSLError
import requests
import json
import threading
import time

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
last_block = None
last_block_per_server = {}
health_status = {}
health_status_per_server = {}
last_request_time_per_server = {}
consecutive_503_per_server = {}


def health_check_thread():
    while True:
        for key, _ in list(health_status_per_server.items()):
            rpc_ip, rpc_port = key.split(":")

            health_status = check_rpc_health(rpc_ip, rpc_port)
            if health_status == 503:
                consecutive_503_per_server[key] = consecutive_503_per_server.get(key, 0) + 1
                if consecutive_503_per_server[key] >= 6:
                    health_status_per_server[key] = 503
                else:
                    health_status_per_server[key] = 200
            else:
                consecutive_503_per_server[key] = 0
                health_status_per_server[key] = health_status

        time.sleep(30)  # Perform health checks every 30 seconds


# Start the health check thread
threading.Thread(target=health_check_thread, daemon=True).start()


@app.route('/health', methods=['GET'])
@cache.cached(timeout=30, key_prefix='health_check')
def health_check():
    rpc_ip = request.headers.get('X-Backend-Server', '127.0.0.1')
    rpc_port = request.headers.get('X-Backend-Port', '8545')
    key = f"{rpc_ip}:{rpc_port}"

    if key not in health_status_per_server:
        health_status_per_server[key] = check_rpc_health(rpc_ip, rpc_port)

    # Update the last request time for the backend server
    last_request_time_per_server[key] = time.time()

    if health_status_per_server[key] == 200:
        return "UP"
    else:
        return "DOWN"

def get_header(headers, key, default=None):
    return next((value for header, value in headers if header.lower() == key.lower()), default)


def update_health_status(rpc_ip, rpc_port):
    global health_status
    key = f"{rpc_ip}:{rpc_port}"
    health_status[key] = check_rpc_health(rpc_ip, rpc_port)


def check_rpc_health(rpc_ip, rpc_port):
    global last_block_per_server

    key = f"{rpc_ip}:{rpc_port}"
    last_block = last_block_per_server.get(key)

    res = 0
    try:
        if rpc_ip.startswith('http://') or rpc_ip.startswith('https://'):
            rpc_address = rpc_ip + ":" + rpc_port
            requests.get(rpc_address)
        else:
            rpc_address = "https://" + rpc_ip + ":" + rpc_port
            try:
                requests.get(rpc_address)
            except SSLError:
                logger.debug(f"SSLError occurred for {rpc_ip}:{rpc_port}, switching to HTTP")
                rpc_address = "http://" + rpc_ip + ":" + rpc_port
            requests.get(rpc_address)

        health_response = requests.get(f"{rpc_address}/health")
        health_data = health_response.json()

        if health_data.get('status') == "Healthy":
            block_number_hex = json.loads(requests.post(rpc_address,
                                           json={'jsonrpc':'2.0','method':'eth_blockNumber','params':[],'id':1})
                                           .text)['result']
            block_number = int(block_number_hex, 16)

            if (last_block is not None) and (block_number > last_block):
                res = 200
            else:
                res = 503
            last_block_per_server[key] = block_number
        else:
            res = 503

    except Exception as e:
        res = 503
        logger.error(f"Error in check_rpc_health: {e}")

    logger.debug(f"Health status for {rpc_ip}:{rpc_port} is {res}")
    return res

if __name__ == "__rpchealth__":
    app.run(host='0.0.0.0', port=9999)
