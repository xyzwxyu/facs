# FACS - CWMP/TR-069 Fast Auto Configuration Server

A high-performance CWMP (CPE WAN Management Protocol) / TR-069 implementation with an interactive CLI for managing and provisioning CPE (Customer Premise Equipment) devices.

## Overview

FACS is a server implementation of the CWMP/TR-069 protocol that allows you to:
- **Manage CPE devices** remotely through the TR-069 protocol
- **Execute RPC methods** on connected devices (reboot, firmware updates, parameter configuration)
- **Monitor device status** and data models in real-time
- **Interactive CLI** for user-friendly device management

## Run the ACS and CLI separately

Run PostgreSQL in Docker, then start the ACS on the host:

```bash
docker compose -f docker/compose/docker-compose.yml up -d postgres
facs
```

Configuration is loaded from the project-root `.env` if it exists. Copy
`.env.example` to `.env` and adjust the database URL and bind address for
your machine. Exported environment variables override the file. To use a
different file, set `FACS_ENV_FILE=/absolute/path/to/facs.env` before running
`facs` or `facs-cli`. The settings and defaults are defined in
[`src/config.py`](src/config.py).

In a second terminal, attach the local CLI with `facs-cli`. If you have not
reinstalled the package since the new entry point was added, use
`python -m src.cli` instead. `facs -cli` starts the ACS and an attached shell;
leaving that shell keeps the ACS running until SIGINT/SIGTERM.

For remote access, configure `FACS_MANAGE_IP`, `FACS_TLS_CERT`, `FACS_TLS_KEY`,
and `FACS_ADMIN_TOKEN` on the ACS, then on the remote machine run:

```bash
export FACS_ADMIN_TOKEN='<server-admin-token>'
facs-cli --url https://acs.example.com:8443
```

## Installation

### Development Installation

Clone and install with development dependencies:

```bash
git clone https://github.com/redsegment/facs.git
cd facs
pip install -e .[dev]
make dev  # Sets up pre-commit hooks
```

### Production Installation

Install as a standalone application:

```bash
pip install .
facs --help
```

## Protocol Support

### CWMP/TR-069

FACS implements the CWMP protocol including:

- **Inform** - Device to ACS notification
- **TransferComplete** - File transfer completion
- **GetRPCMethods** - Query available RPC methods
- **SetParameterValues** - Configure device parameters
- **GetParameterValues** - Query device parameters
- **GetParameterNames** - List device parameters
- **Reboot** - Remotely reboot device
- **Upgrade** - Remotely upgrade device
- **FactoryReset** - Remotely reset device

### SOAP Message Handling

Full SOAP/XML message parsing and generation with:

- Proper namespace handling
- CWMP-specific extensions
- Fault message support


## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make changes following code style guidelines
4. Run tests and checks:
   ```bash
   make lint
   make type
   make test
   ```
5. Commit with clear messages
6. Push to your fork
7. Open a Pull Request
