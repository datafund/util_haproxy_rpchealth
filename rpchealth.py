import logging
import json
import os
import asyncio
import aiohttp
from aiohttp import web
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

routes = web.RouteTableDef()
SERVER_DATA_FILE = "server_data.json"
HEALTH_CHECK_INTERVAL = 60
file_write_lock = asyncio.Lock()

backend_servers = set()
health_check_task = None  # Task for the periodic health check

class RPCError(Exception):
    pass

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
    global health_check_task
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{rpc_address}/health", timeout=3) as response:
                if response.status == 200:
                    health_data = await response.json()

                    if health_data.get("status") == "Healthy":
                        node_health = health_data.get("entries", {}).get("node-health", {})
                        is_syncing = node_health.get("data", {}).get("IsSyncing", False)

                        if is_syncing:
                            return 503, None

                        try:
                            async with session.post(
                                rpc_address,
                                json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
                            ) as block_response:
                                block_data = await block_response.json()
                                if "result" not in block_data:
                                    logger.error(f"Invalid block number response from {rpc_address}")
                                    return 503, None

                                block_number_hex = block_data["result"]
                                block_number = int(block_number_hex, 16)

                                if block_number == 0:
                                    return 503, None

                                if key in server_data['last_block'] and block_number <= server_data['last_block'][key]:
                                    return 503, None

                                # Return 200 and the block number
                                return 200, block_number

                        except aiohttp.ClientError as e:
                            logger.error(f"Error occurred while retrieving block number from {rpc_address}: {e}")
                            return 503, None

                    logger.error(f"Response from {rpc_address} was not healthy")
                    return 503, None
                else:
                    logger.error(f"Response from {rpc_address} was not 200")
                    health_check_task=None
                    return 503, None

            # Return 503 and None if the status is not healthy or if syncing
            return 503, None

        except aiohttp.ClientError as e:
            logger.error(f"Error occurred while checking health status of {rpc_address}: {e}")
            health_check_task=None
            return 503, None


async def save_server_data(server_data):
    async with file_write_lock:  # Acquire the lock before writing to the file
        with open(SERVER_DATA_FILE, 'w') as outfile:
            json.dump(server_data, outfile)
            outfile.flush()  # Flush the internal buffer
            os.fsync(outfile.fileno())  # Sync the file to disk

async def load_server_data():
    if os.path.isfile(SERVER_DATA_FILE):
        with open(SERVER_DATA_FILE, 'r') as infile:
            return json.load(infile)
    return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}}


async def update_health_status():
    global health_check_task
    while True:
        server_data = await load_server_data()
        for key in server_data['servers']:
            rpc_address = server_data['servers'][key]
            old_status = server_data['health_status'].get(key, 200)

            try:
                health_status, block_number = await check_rpc_health(rpc_address, key, server_data)

                if health_status != old_status:
                    message = f"Health status for {rpc_address} changed from {old_status} to {health_status}"
                    logger.info(message)
                    await send_telegram_notification(message)

                if block_number is not None:
                    if key not in server_data['stale_count']:
                        server_data['stale_count'][key] = 0

                    # Check if block number is greater than the last recorded block number
                    if key not in server_data['last_block'] or block_number > server_data['last_block'][key]:
                        server_data['last_block'][key] = block_number
                        server_data['stale_count'][key] = 0
                    else:
                        server_data['stale_count'][key] += 1

                    if server_data['stale_count'][key] >= 2:
                        health_status = 503

                # Handle scenarios where block number is 0 or None
                if block_number is None or block_number == 0:
                    health_status = 503

                server_data['health_status'][key] = health_status

                if health_status == 503 and old_status == 200:
                    reason = "Stale Block" if server_data['stale_count'][key] >= 3 else "Unhealthy Status"
                    logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status} ({reason})")

                elif health_status == 200 and old_status != 200:
                    logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status}")
                    server_data['stale_count'][key] = 0

            except (ConnectionRefusedError, TimeoutError, RPCError) as e:
                # Mark as unhealthy due to network or RPC issues
                health_status = 503
                logger.error(f"Error checking health status of {rpc_address}: {e}")
                server_data['health_status'][key] = health_status

            except Exception as e:
                # Record error but don't mark as unhealthy
                logger.error(f"Unexpected error checking health status of {rpc_address}: {e}")

            # Save server data after each server update (including exceptions)
            await save_server_data(server_data)

        # Check if any backend falls behind in last_block
        max_block = max(server_data['last_block'].values())
        min_block = min(server_data['last_block'].values())
        if max_block - min_block > 200:
            # Send a Telegram alert if any backend falls behind
            message = "RPC Backend Alert:\n"
            for k, v in server_data['last_block'].items():
                if max_block - v > 10:
                    message += f"Backend {k} is behind in last_block.\n"
            await send_telegram_notification(message)

        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


async def send_telegram_notification(message):
    telegram_api_key = os.getenv("TELEGRAM_API_KEY")  # Load the Telegram API key from environment variable
    chat_id = os.getenv("TELEGRAM_CHAT_ID")  # Load the Telegram chat ID from environment variable
    host = os.getenv("HEALTH_HOST", "$HOSTNAME")
    
    url = f"https://api.telegram.org/bot{telegram_api_key}/sendMessage"
    full_message = host + " " + message
    payload = {
        "chat_id": chat_id,
        "text": full_message
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                logger.info("Telegram notification sent successfully")
            else:
                logger.error("Failed to send Telegram notification")

@routes.get('/health')
async def health_check(request):
    global health_check_task
    rpc_ip = request.headers.get("X-Backend-Server", "127.0.0.1")
    rpc_port = request.headers.get("X-Backend-Port", "8545")
    key = f"{rpc_ip}:{rpc_port}"

    server_data = await load_server_data()

    if key not in server_data["health_status"]:
        server_data["health_status"][key] = 200
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
    try:
        app = web.Application()
        app.add_routes(routes)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 9999)
        await site.start()
        logger.info("Server started on http://0.0.0.0:9999")

        # Keep the server running until interrupted
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error in __main__: {e}")
