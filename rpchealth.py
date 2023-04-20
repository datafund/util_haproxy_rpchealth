import asyncio
import aiohttp
from aiohttp import web
import json
import os
import asyncio
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

routes = web.RouteTableDef()
SERVER_DATA_FILE = "server_data.json"
HEALTH_CHECK_INTERVAL = 60
health_status_lock = asyncio.Lock()

backend_servers = set()
health_check_task = None  # Task for the periodic health check

async def get_rpc_address(rpc_ip, rpc_port):
    if rpc_ip and rpc_port:
        protocol = "http"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"https://{rpc_ip}:{rpc_port}", timeout=3) as response:
                    if response.status == 200:
                        protocol = "https"
        except (aiohttp.ClientError, ValueError):
            pass

        return f"{protocol}://{rpc_ip}:{rpc_port}"
    else:
        return None

async def check_rpc_health(rpc_address, key, server_data):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{rpc_address}/health") as response:
                if response.status == 200:
                    health_data = await response.json()

                    if health_data.get("status") == "Healthy":
                        async with session.post(rpc_address, json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}) as block_response:
                            block_data = await block_response.json()
                            block_number_hex = block_data["result"]
                            block_number = int(block_number_hex, 16)

                            last_block = server_data["last_block"].get(key, 0)
                            stale_block_count = server_data["stale_block_count"].get(key, 0)

                            if block_number > last_block:
                                server_data["stale_block_count"][key] = 0
                                return 200, block_number
                            else:
                                if stale_block_count < 5:
                                    server_data["stale_block_count"][key] = stale_block_count + 1
                                    return 200, block_number
                                else:
                                    return 503, block_number
        except aiohttp.ClientError as e:
            return 503, None



async def save_server_data(server_data):
    with open(SERVER_DATA_FILE, 'w') as outfile:
        json.dump(server_data, outfile)

async def load_server_data():
    if os.path.isfile(SERVER_DATA_FILE):
        with open(SERVER_DATA_FILE, 'r') as infile:
            return json.load(infile)
    return {"health_status": {}, "servers": {}, "last_block": {}, "stale_block_count": {}}

async def update_health_status():
    global health_check_task
    while True:
        server_data = await load_server_data()
        for key in server_data['servers']:
            rpc_address = server_data['servers'][key]
            health_status, block_number = await check_rpc_health(rpc_address, key, server_data)
            server_data['health_status'][key] = health_status

            # Check if block_number is not None and if it's greater than the last_block
            #if block_number is not None and (key not in server_data['last_block'] or block_number > server_data['last_block'][key]):
            server_data['last_block'][key] = block_number

        await save_server_data(server_data)
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


@routes.get('/health')
async def health_check(request):
    global health_check_task
    rpc_ip = request.headers.get("X-Backend-Server", "127.0.0.1")
    rpc_port = request.headers.get("X-Backend-Port", "8545")
    key = f"{rpc_ip}:{rpc_port}"

    server_data = await load_server_data()

    if key not in server_data["health_status"]:
        server_data["health_status"][key] = 503
        rpc_address = await get_rpc_address(rpc_ip, rpc_port)
        server_data["servers"][key] = rpc_address
        await save_server_data(server_data)

    if health_check_task is None:
        health_check_task = asyncio.create_task(update_health_status())

    if server_data["health_status"][key] == 200:
        return web.Response(text="UP")
    else:
        return web.Response(text="DOWN")

async def main():
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 9999)
    await site.start()
    print("Server started on http://0.0.0.0:9999")

    # Keep the server running until interrupted
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
