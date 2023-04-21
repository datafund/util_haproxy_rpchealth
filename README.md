# Asyncio RPC Health Checker

This is an asynchronous RPC health checker built on top of the `aiohttp` library in Python. It periodically checks the health status of an RPC server and saves it to a local file. It also provides an HTTP endpoint `/health` that returns the health status of the server based on the RPC health checks.

## Installation

### Using pip

To install the required dependencies, run:


### Using Docker

You can also use the pre-built Docker image `darkobas/rpchealth`. To start the health checker server using Docker, run:
<details>
  <summary>Copy Docker command</summary>
  <code>docker run -p 9999:9999 darkobas/rpchealth</code>
</details>

This will start the server on `http://0.0.0.0:9999`.

### Using Helm

Helm charts are also available in the `helm` directory of this repository. To use the Helm charts, run:
<details>
  <summary>Copy Helm command</summary>
  <code>helm install rpchealth ./helm/rpchealth</code>
</details>

This will install the `rpchealth` chart with the name `rpchealth`.

## Usage

To start the health checker server, run:


This will start the server on `http://0.0.0.0:9999`.

### Endpoints

The server provides a single endpoint:

#### `GET /health`

This endpoint checks the health status of the RPC server and returns the result.

##### Request headers

- `X-Backend-Server`: the IP address of the RPC server. Defaults to `127.0.0.1`.
- `X-Backend-Port`: the port number of the RPC server. Defaults to `8545`.

##### Response

- `UP`: The server is healthy.
- `DOWN`: The server is not healthy.

## Configuration

The configuration options for the health checker are set through the following constants:

- `SERVER_DATA_FILE`: The path to the file where the server data is saved. Default is `server_data.json`.
- `HEALTH_CHECK_INTERVAL`: The interval (in seconds) between health checks. Default is `60`.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.

