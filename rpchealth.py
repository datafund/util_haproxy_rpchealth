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
STALE_THRESHOLD = 5  # Number of checks before a server is considered stale
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds
REMOVE_AFTER_FAILURES = 5  # Remove a server after this many consecutive failures

file_write_lock = asyncio.Lock()

health_check_task = None  # Store the health check task


class RPCError(Exception):
    """Custom exception for RPC errors."""
    pass


async def get_rpc_address(rpc_ip, rpc_port):
    """Attempts to connect to the RPC server using http and https."""
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
    """Checks the health of an RPC server."""
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

                    # Check if block number is decreasing
                    if key in server_data['last_block'] and server_data['last_block'][key] is not None and block_number < server_data['last_block'][key]:
                        logger.warning(f"Block number decreased for {rpc_address}")
                        return 503, None  # Treat decreasing block number as unhealthy

                    return 200, block_number

            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as e:
                logger.error(f"Error occurred while retrieving block number from {rpc_address}: {e}")
                return 503, None

        except Exception as e:
            logger.exception(f"Unexpected error in check_rpc_health for {rpc_address}: {e}")
            return 503, None


async def save_server_data(server_data):
    """Saves the server data to the JSON file."""
    async with file_write_lock:
        try:
            with open(SERVER_DATA_FILE, 'w') as outfile:
                json.dump(server_data, outfile, indent=4)
                outfile.flush()  # Ensure data is written to disk
                os.fsync(outfile.fileno())  # More robust flushing
        except Exception as e:
            logger.error(f"Error saving server data: {e}")

async def load_server_data():
    """Loads the server data from the JSON file."""
    try:
        if os.path.isfile(SERVER_DATA_FILE):
            with open(SERVER_DATA_FILE, 'r') as infile:
                return json.load(infile)
        return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}, "failure_count": {}}
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading server data: {e}, returning default data")
        return {"health_status": {}, "servers": {}, "last_block": {}, "stale_count": {}, "failure_count": {}}


async def ensure_health_check_task():
    """Ensures the health check task is running."""
    global health_check_task
    if health_check_task is None or health_check_task.done():
        health_check_task = asyncio.create_task(update_health_status())
        logger.info("Health check task started or restarted.")

async def update_health_status():
    """Periodically checks the health status of all servers."""
    while True:
        server_data = await load_server_data()

        # Initialize failure_count if it doesn't exist (for older data files)
        if "failure_count" not in server_data:
            server_data["failure_count"] = {}

        for key in list(server_data['servers'].keys()):  # Iterate on a copy to allow deletion
            rpc_address = server_data['servers'].get(key)
            old_status = server_data['health_status'].get(key, 200)  # Default to 200 if not found
            
            # Attempt to get a valid RPC address if the current one is a placeholder
            if rpc_address == key:  # Check if it's a placeholder
                logger.debug(f"Attempting to resolve placeholder address for {key}")
                rpc_ip, rpc_port = key.split(":")
                rpc_address = await get_rpc_address(rpc_ip, rpc_port)
                if rpc_address:
                    logger.info(f"Resolved placeholder address for {key} to {rpc_address}")
                    server_data['servers'][key] = rpc_address  # Update with the real address
                    server_data["failure_count"][key] = 0  # reset failure count
                else:
                    server_data["failure_count"][key] = server_data["failure_count"].get(key, 0) + 1

            if rpc_address is None or rpc_address == key:  # still couldn't get rpc address
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
                    await save_server_data(server_data)  # Save failure count
                continue

            # Default new status to unhealthy until proven otherwise
            health_status = 503
            block_number = None
            max_block = 0
            health_reason = "OK"

            # Try to get the health status from the server with retries
            for attempt in range(MAX_RETRIES):
                try:
                    initial_health_status, initial_block_number = await check_rpc_health(rpc_address, key, server_data)
                    
                    # Set the health status and block number from the initial check
                    health_status = initial_health_status
                    block_number = initial_block_number
                    
                    # Process block number if valid
                    if block_number is not None:
                        if key not in server_data['stale_count']:
                            server_data['stale_count'][key] = 0  # Initialize if not present

                        # Update last_block and reset stale_count if block number increased
                        if key not in server_data['last_block'] or block_number > (server_data['last_block'].get(key) or 0):
                            server_data['last_block'][key] = block_number
                            server_data['stale_count'][key] = 0
                        else:
                            server_data['stale_count'][key] += 1
                            if server_data['stale_count'][key] >= STALE_THRESHOLD:
                                health_status = 503
                                health_reason = "Stale block"
                    
                    # Check for invalid block number
                    if block_number is None or block_number == 0:
                        health_status = 503
                        health_reason = "Invalid block number"
                    
                    break  # Exit retry loop on success

                except (ConnectionRefusedError, TimeoutError, RPCError, OSError) as e:
                    logger.error(f"Attempt {attempt + 1} failed for {rpc_address}: {e}")
                    if attempt == MAX_RETRIES - 1:
                        health_status = 503
                        health_reason = f"Failed after {MAX_RETRIES} retries"
                    else:
                        await asyncio.sleep(RETRY_DELAY)
                except Exception as e:
                    logger.exception(f"Unexpected error checking health status of {rpc_address}: {e}")
                    health_status = 503
                    health_reason = f"Unexpected error: {str(e)}"
                    break  # Exit retry loop for unexpected exceptions

            # Final check: Block difference against other servers
            # KEY FIX: Only update health status for block difference if difference is significant
            if health_status == 200:  # Only check block difference if otherwise healthy
                valid_blocks = [b for b in server_data.get('last_block', {}).values() if b is not None]
                if valid_blocks:  # Only proceed if there are valid block numbers
                    max_block = max(valid_blocks)
                    current_server_block = server_data['last_block'].get(key)  # Get current server's block

                    # KEY FIX: Changed threshold from 50 to a larger value (use at least 100)
                    # Or, if you want to keep it at 50, ensure we're using the right comparison
                    if current_server_block is not None and max_block - current_server_block > 100:
                        health_status = 503  # Mark as unhealthy
                        health_reason = f"Block difference (behind by {max_block - current_server_block})"
                        # KEY FIX: Only increment failure count for large block differences
                        server_data['failure_count'][key] = server_data["failure_count"].get(key, 0) + 1
            
            # KEY FIX: Update failure count based on final health status
            if health_status == 200:
                server_data["failure_count"][key] = 0  # reset failure count on success
            else:
                # Only increment if it wasn't already incremented for block difference
                if health_reason != f"Block difference (behind by {max_block - current_server_block})":
                    server_data['failure_count'][key] = server_data["failure_count"].get(key, 0) + 1

            # Check for server removal due to failures
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
                continue  # Skip the rest for this server

            # Now that all health checks are complete, update the status and notify if changed
            if health_status != old_status:
                server_data['health_status'][key] = health_status
                
                if health_status == 503 and old_status == 200:
                    logger.warning(f"Health status for {rpc_address} changed from {old_status} to {health_status} ({health_reason})")
                else:
                    logger.info(f"Health status for {rpc_address} changed from {old_status} to {health_status} ({health_reason})")
                
                # Send notification for status change
                message = f"Health status for {rpc_address} changed from {old_status} to {health_status} ({health_reason})"
                await send_telegram_notification(message)
            else:
                # Status didn't change, but still update in the data structure
                server_data['health_status'][key] = health_status
                
                # If status is 503 due to block difference but was already 503, log a warning without notification
                if health_status == 503 and health_reason.startswith("Block difference"):
                    logger.warning(f"Server {key} remains unhealthy due to {health_reason}")

            await save_server_data(server_data)

        # --- Check for large block differences (with notification on change) ---
        if server_data.get('last_block'):
            valid_blocks = [b for b in server_data['last_block'].values() if b is not None]
            if valid_blocks:  # Only proceed if there are valid blocks
                max_block = max(valid_blocks)
                min_block = min(valid_blocks)

                current_block_diff_large = max_block - min_block > (STALE_THRESHOLD * 15)  # True if large difference

                # Get the previous large block difference status (default to False if it doesn't exist)
                previous_block_diff_large = server_data.get('block_diff_large', False)

                if current_block_diff_large != previous_block_diff_large:  # Only proceed if status changed
                    server_data['block_diff_large'] = current_block_diff_large  # Update the stored status
                    await save_server_data(server_data)  # save the status.

                    if current_block_diff_large:  # if it is currently large
                        message = "RPC Backend Alert:\n"
                        for k, v in server_data['last_block'].items():
                            if v is not None and max_block - v > (STALE_THRESHOLD * 10):
                                message += f"Backend {k} is behind in last_block.\n"
                        await send_telegram_notification(message)
        # --- End of block difference check ---

        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

async def send_telegram_notification(message):
    """Sends a notification to Telegram."""
    telegram_api_key = os.getenv("TELEGRAM_API_KEY")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    host = os.getenv("HEALTH_HOST", "$HOSTNAME")  # Default to hostname if not set

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
    """Handles health check requests, reporting UP or DOWN."""
    rpc_ip = request.headers.get("X-Backend-Server", "127.0.0.1")
    rpc_port = request.headers.get("X-Backend-Port", "8545")

    # --- Add check to potentially overwrite based on X-Backend-Server ---
    backend_server_header = request.headers.get("X-Backend-Server")
    if backend_server_header and ':' in backend_server_header:
        parts = backend_server_header.rsplit(':', 1)
        potential_host = parts[0]
        potential_port = parts[1]

        if potential_port.isdigit():
            rpc_ip = potential_host
            rpc_port = potential_port
    key = f"{rpc_ip}:{rpc_port}"

    server_data = await load_server_data()

    # If the server is not in the list, add it.  This handles initial requests.
    if key not in server_data["servers"]:
        rpc_address = await get_rpc_address(rpc_ip, rpc_port)
        if rpc_address:  # Successfully got an address
            server_data["servers"][key] = rpc_address
            initial_health_status, initial_block_number = await check_rpc_health(rpc_address, key, server_data)  # Get initial values

            # --- INITIAL BLOCK DIFFERENCE CHECK (using STALE_THRESHOLD) ---
            if initial_block_number is not None and server_data.get('last_block'):
                valid_existing_blocks = [b for b in server_data['last_block'].values() if b is not None]
                max_existing_block = max(valid_existing_blocks, default=0)
                if max_existing_block - initial_block_number > STALE_THRESHOLD:
                    initial_health_status = 503
                    logger.warning(f"Server {key} added, but marked unhealthy (initial block difference).")

            server_data["health_status"][key] = initial_health_status
            server_data["last_block"][key] = initial_block_number if initial_block_number is not None else 0  # Save initial block
            server_data["failure_count"][key] = 0 if initial_health_status == 200 else 1  # Initialize based on health
            server_data["stale_count"][key] = 0
        else:  # Failed to get an address on initial add
            # Use the ip:port as a placeholder
            server_data["servers"][key] = key
            server_data["health_status"][key] = 503
            server_data["failure_count"][key] = 1  # Start with a failure count of 1
        await save_server_data(server_data)

    await ensure_health_check_task()  # Make sure the background task is running

    if server_data["health_status"].get(key, 503) == 200:  # Default to unhealthy if not found
        return web.Response(text="UP")
    else:
        return web.Response(text="DOWN")


async def main():
    """Starts the web server and the background health check task."""
    try:
        app = web.Application()
        app.add_routes(routes)
        runner = web.AppRunner(app, access_log=None)  # Disable access logs for cleaner output
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 9999)
        await site.start()
        logger.info("Server started on http://0.0.0.0:9999")

        while True:
            await asyncio.sleep(1)  # Keep the main loop running
    except Exception as e:
        logger.error(f"Error in main: {e}")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Error in __main__: {e}")

