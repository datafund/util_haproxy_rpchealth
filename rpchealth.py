import logging
import asyncio
import aiohttp
import json
import os
from aiohttp import web

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
        for protocol in ["http", "https"]:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{protocol}://{rpc_ip}:{rpc_port}", timeout=10) as response:
                        if response.status == 200:
                            return f"{protocol}://{rpc_ip}:{rpc_port}"  # Success!
            except (aiohttp.ClientError, OSError, ValueError) as e:
                logger.warning(f"Error connecting to {rpc_ip}:{rpc_port} over {protocol}: {e}")

        return None
    else:
        return None


async def check_rpc_health(rpc_address, key, server_data):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{rpc_address}/health", timeout=30) as response:
                if response.status == 200:
                    health_data = await response.json()

                    if health_data.get("status") == "Healthy":
                        node_health = health_data.get("entries", {}).get("node-health", {})
                        is_syncing = node_health.get("data", {}).get("IsSyncing", False)

                        if is_syncing:
                            logger.info(f"Server {rpc_address} is syncing.")
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
                                    logger.error(f"Block number was 0 for {rpc_address}")
                                    return 503, None

                                if key in server_data['last_block'] and block_number < server_data['last_block'][key]:
                                    return 503, None

                                return 200, block_number

                        except (aiohttp.ClientError, OSError) as e:
                            logger.error(f"Error occurred while retrieving block number from {rpc_address}: {e}")
                            return 503, None

                    logger.error(f"Response from {rpc_address} was not healthy")
                    return 503, None
                else:
                    logger.error(f"Response from {rpc_address} was not 200")
                    return 503, None

        except (aiohttp.ClientError, OSError) as e:
            logger.error(f"Error occurred while checking health status of {rpc_address}: {e}")
            return 503, None


async def save_server_data(server_data):
    async with file_write_lock:
        with open(SERVER_DATA_FILE, 'w') as outfile:
            json.dump(server_data, outfile)
            outfile.flush()
            os.fsync(outfile.fileno())


async def load_server_data():
    if os.path.isfile(SERVER_DATA_FILE):
        with open(SERVER_DATA_FILE, 'r') as infile:
            return json.load(infile)
    return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}}


async def ensure_health_check_task():
    global health_check_task
    if health_check_task is None or health_check_task.done():
        health_check_task = asyncio.create_task(update_health_status())
        logger.info("Health check task started or restarted.")


async def update_health_status():
    while True:
        server_data = await load_server_data()
        for key in server_data['servers']:
            rpc_address = server_data['servers'][key]
            old_status = server_data['health_status'].get(key, 200)
            if rpc_address is not None:
                try:
                    health_status, block_number = await check_rpc_health(rpc_address, key, server_data)

                    if health_status != old_status:
                        message = f"Health status for {rpc_address} changed from {old_status} to {health_status}"
                        logger.info(message)
                        await send_telegram_notification(message)

                    if block_number is not None:
                        if key not in server_data['stale_count']:
                            server_data['stale_count'][key] = 0

                        if key not in server_data['last_block'] or block_number > server_data['last_block'][key]:
                            server_data['last_block'][key] = block_number
                            server_data['stale_count'][key] = 0
                        else:
                            server_data['stale_count'][key] += 1

                        if server_data['stale_count'][key] >= 100:
                            health_status = 503

                    if block_number is None or block_number == 0:
                        logger.info(f"Block number was 0 or None for {rpc_address}")
                        health_status = 503

                    server_data['health_status'][key] = health_status

                    if health_status == 503 and old_status == 200:
                        reason = "Stale Block" if server_data['stale_count'][key] >= 3 else "Unhealthy Status"
                        logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status} ({reason})")

                    elif health_status == 200 and old_status != 200:
                        logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status}")
                        server_data['stale_count'][key] = 0

                except (ConnectionRefusedError, TimeoutError, RPCError, OSError) as e:
                    health_status = 503
                    logger.error(f"Error checking health status of {rpc_address}: {e}")
                    server_data['health_status'][key] = health_status

                except Exception as e:
                    logger.error(f"Unexpected error checking health status of {rpc_address}: {e}")

                await save_server_data(server_data)

            else:
                logger.info(f"Health check skipped for {rpc_address}")
                health_status = 503

        await save_server_data(server_data)

        if server_data['last_block']:
            max_block = max(server_data['last_block'].values())
            min_block = min(server_data['last_block'].values())
            if max_block - min_block > 200:
                message = "RPC Backend Alert:\n"
                for k, v in server_data['last_block'].items():
                    if max_block - v > 100:
                        message += f"Backend {k} is behind in last_block.\n"
                await send_telegram_notification(message)

        await asyncio.sleep(HEALTH_CHECK_INTERVAL)


async def send_telegram_notification(message):
    telegram_api_key = os.getenv("TELEGRAM_API_KEY")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
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
    rpc_ip = request.headers.get("X-Backend-Server", "127.0.0.1")
    rpc_port = request.headers.get("X-Backend-Port", "8545")
    key = f"{rpc_ip}:{rpc_port}"

    server_data = await load_server_data()

    if key not in server_data["health_status"]:
        rpc_address = await get_rpc_address(rpc_ip, rpc_port)
        server_data["servers"][key] = rpc_address
        server_data["health_status"][key] = 503  # Default to unhealthy until proven healthy
        await save_server_data(server_data)

    await ensure_health_check_task()

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

        while True:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error in __main__: {e}")

