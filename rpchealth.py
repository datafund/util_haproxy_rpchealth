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
STALE_THRESHOLD = 3
MAX_RETRIES = 3
RETRY_DELAY = 5
REMOVE_AFTER_FAILURES = 5

file_write_lock = asyncio.Lock()

health_check_task = None


class RPCError(Exception):
    pass


async def get_rpc_address(rpc_ip, rpc_port):
    if not (rpc_ip and rpc_port):
        return None

    for protocol in ["http", "https"]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{protocol}://{rpc_ip}:{rpc_port}", timeout=10) as response:
                    if response.status == 200:
                        return f"{protocol}://{rpc_ip}:{rpc_port}"
        except (aiohttp.ClientError, OSError, ValueError, asyncio.TimeoutError) as e:
            logger.warning(f"Error connecting to {rpc_ip}:{rpc_port} over {protocol}: {e}")

    logger.error(f"Failed to connect to {rpc_ip}:{rpc_port} after trying both http and https.")
    return None


async def check_rpc_health(rpc_address, key, server_data):
    async with aiohttp.ClientSession() as session:
        try:
            # Health Endpoint Check
            try:
                async with session.get(f"{rpc_address}/health", timeout=30) as response:
                    if response.status != 200:
                        logger.error(f"Health check at {rpc_address}/health returned status: {response.status}")
                        return 503, None

                    try:
                        health_data = await response.json()
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from {rpc_address}/health")
                        return 503, None

                    if health_data.get("status") != "Healthy":
                        logger.warning(f"Server {rpc_address} reported unhealthy status.")
                        return 503, None

                    node_health = health_data.get("entries", {}).get("node-health", {})
                    is_syncing = node_health.get("data", {}).get("IsSyncing", False)

                    if is_syncing:
                        logger.info(f"Server {rpc_address} is syncing.")
                        return 503, None

            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
                logger.error(f"Error during health endpoint check for {rpc_address}: {e}")
                return 503, None

            # Block Number Check
            try:
                async with session.post(
                    rpc_address,
                    json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
                    timeout=30
                ) as block_response:
                    if block_response.status != 200:
                         logger.error(f"Block number request to {rpc_address} returned status: {block_response.status}")
                         return 503, None
                    try:
                        block_data = await block_response.json()
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from {rpc_address} for block number.")
                        return 503, None

                    if "result" not in block_data:
                        logger.error(f"Invalid block number response from {rpc_address}: missing 'result' field. Response: {block_data}")
                        return 503, None

                    block_number_hex = block_data["result"]
                    try:
                        block_number = int(block_number_hex, 16)
                    except ValueError:
                        logger.error(f"Invalid block number format from {rpc_address}: {block_number_hex}")
                        return 503, None

                    if block_number == 0:
                        logger.error(f"Block number was 0 for {rpc_address}")
                        return 503, None
                    if key in server_data['last_block'] and block_number < server_data['last_block'][key]:
                        logger.warning(f"Block number decreased for {rpc_address}")
                        return 503, None
                    return 200, block_number

            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
                logger.error(f"Error occurred while retrieving block number from {rpc_address}: {e}")
                return 503, None

        except Exception as e:
            logger.exception(f"Unexpected error in check_rpc_health for {rpc_address}: {e}")
            return 503, None


async def save_server_data(server_data):
    async with file_write_lock:
        try:
            with open(SERVER_DATA_FILE, 'w') as outfile:
                json.dump(server_data, outfile, indent=4)
                outfile.flush()
                os.fsync(outfile.fileno())
        except Exception as e:
            logger.error(f"Error saving server data: {e}")

async def load_server_data():
    try:
        if os.path.isfile(SERVER_DATA_FILE):
            with open(SERVER_DATA_FILE, 'r') as infile:
                return json.load(infile)
        return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}, "failure_count": {}}
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading server data: {e}, returning default data")
        return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}, "failure_count": {}}


async def ensure_health_check_task():
    global health_check_task
    if health_check_task is None or health_check_task.done():
        health_check_task = asyncio.create_task(update_health_status())
        logger.info("Health check task started or restarted.")



async def update_health_status():
    while True:
        server_data = await load_server_data()

        if "failure_count" not in server_data:
            server_data["failure_count"] = {}

        for key in list(server_data['servers'].keys()):
            rpc_address = server_data['servers'].get(key)
            old_status = server_data['health_status'].get(key, 200)

            # Attempt to get a valid RPC address if the current one is a placeholder
            if rpc_address == key : # Check if it's a placeholder
                logger.debug(f"Attempting to resolve placeholder address for {key}")
                rpc_ip, rpc_port = key.split(":")
                rpc_address = await get_rpc_address(rpc_ip, rpc_port)
                if rpc_address:
                    logger.info(f"Resolved placeholder address for {key} to {rpc_address}")
                    server_data['servers'][key] = rpc_address  # Update with the real address
                    server_data["failure_count"][key] = 0 #reset failure count
                else:
                     server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1

            if rpc_address is None or rpc_address == key: #still couldn't get rpc address
                logger.warning(f"Skipping health check for {key} due to invalid RPC address.")
                # Increment failure count and potentially remove
                server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1
                if server_data["failure_count"][key] >= REMOVE_AFTER_FAILURES:
                    logger.info(f"Removing server {key} due to persistent invalid RPC address.")
                    del server_data["servers"][key]
                    if key in server_data["health_status"]:
                        del server_data["health_status"][key]
                    if key in server_data["last_block"]:
                        del server_data["last_block"][key]
                    if key in server_data["stale_count"]:
                        del server_data["stale_count"][key]
                    if key in server_data["failure_count"]:
                        del server_data["failure_count"][key]
                    await save_server_data(server_data)
                else:
                    await save_server_data(server_data) # Save failure count
                continue


            for attempt in range(MAX_RETRIES):
                try:
                    health_status, block_number = await check_rpc_health(rpc_address, key, server_data)

                    if health_status != old_status:
                        message = f"Health status for {rpc_address} changed from {old_status} to {health_status}"
                        logger.info(message)
                        await send_telegram_notification(message)

                    if health_status == 200:
                        server_data["failure_count"][key] = 0
                    else:
                        server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1

                    if block_number is not None:
                        if key not in server_data['stale_count']:
                            server_data['stale_count'][key] = 0

                        if key not in server_data['last_block'] or block_number > server_data['last_block'][key]:
                            server_data['last_block'][key] = block_number
                            server_data['stale_count'][key] = 0
                        else:
                            server_data['stale_count'][key] += 1

                        if server_data['stale_count'][key] >= STALE_THRESHOLD:
                            health_status = 503

                    if block_number is None or block_number == 0:
                        logger.info(f"Block number was 0 or None for {rpc_address}")
                        health_status = 503

                    server_data['health_status'][key] = health_status

                    if health_status == 503 and old_status == 200:
                        reason = "Stale Block" if server_data['stale_count'][key] >= STALE_THRESHOLD else "Unhealthy Status"
                        logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status} ({reason})")
                    elif health_status == 200 and old_status != 200:
                         logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status}")
                         server_data['stale_count'][key] = 0

                    break

                except (ConnectionRefusedError, TimeoutError, RPCError, OSError) as e:
                    logger.error(f"Attempt {attempt + 1} failed for {rpc_address}: {e}")
                    server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1
                    if attempt == MAX_RETRIES - 1:
                        health_status = 503
                        server_data['health_status'][key] = health_status
                        logger.error(f"Marking {rpc_address} as unhealthy after multiple retries.")
                    else:
                        await asyncio.sleep(RETRY_DELAY)
                except Exception as e:
                    logger.exception(f"Unexpected error checking health status of {rpc_address}: {e}")
                    server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1
                    health_status = 503
                    server_data['health_status'][key] = health_status
                    break

            if server_data["failure_count"].get(key, 0) >= REMOVE_AFTER_FAILURES:
                logger.info(f"Removing server {key} due to persistent failures.")
                del server_data["servers"][key]
                if key in server_data["health_status"]:
                    del server_data["health_status"][key]
                if key in server_data["last_block"]:
                    del server_data["last_block"][key]
                if key in server_data["stale_count"]:
                    del server_data["stale_count"][key]
                if key in server_data["failure_count"]:
                    del server_data["failure_count"][key]

            await save_server_data(server_data)


        if server_data.get('last_block'):
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

    if not telegram_api_key or not chat_id:
        logger.error("Telegram API key or chat ID not configured. Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{telegram_api_key}/sendMessage"
    full_message = f"{host} {message}"
    payload = {
        "chat_id": chat_id,
        "text": full_message
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info("Telegram notification sent successfully")
                else:
                    logger.error(f"Failed to send Telegram notification: Status {response.status}, Response: {await response.text()}")
    except Exception as e:
        logger.error(f"Error sending Telegram notification: {e}")


@routes.get('/health')
async def health_check(request):
    rpc_ip = request.headers.get("X-Backend-Server", "127.0.0.1")
    rpc_port = request.headers.get("X-Backend-Port", "8545")
    key = f"{rpc_ip}:{rpc_port}"

    server_data = await load_server_data()

    if key not in server_data["servers"]:
        rpc_address = await get_rpc_address(rpc_ip, rpc_port)
        if rpc_address:  # Successfully got an address
            server_data["servers"][key] = rpc_address
            initial_health_status, _ = await check_rpc_health(rpc_address, key, server_data)
            server_data["health_status"][key] = initial_health_status
            server_data["failure_count"][key] = 0  # Initialize failure count
        else:  # Failed to get an address on initial add
            # Use the ip:port as a placeholder
            server_data["servers"][key] = key
            server_data["health_status"][key] = 503
            server_data["failure_count"][key] = 1  # Start with a failure count of 1
        await save_server_data(server_data)

    await ensure_health_check_task()

    if server_data["health_status"].get(key, 503) == 200:
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
