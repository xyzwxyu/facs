# Docker Directory

This directory contains all Docker configuration files for FACS deployment with PostgreSQL.

## Files

- **Dockerfile** - Multi-stage Docker image build for FACS
- **compose/docker-compose.yml** - PostgreSQL production setup
- **compose/docker-compose.dev.yml** - Development overrides (live code reload)
- **config/nginx.conf** - Nginx reverse proxy configuration
- **config/.env.example** - Environment variables template
- **database/init-db.sql** - PostgreSQL database initialization script
- **scripts/docker.sh** - Docker management helper script
- **.dockerignore** - Files to exclude from Docker build context

## Quick Start

## Make Commands

```bash
make docker-build            # Build Docker image
make docker-facs-up          # Start with live reload
make docker-facs-stop        # Stop development mode
make docker-facs-logs        # View development logs
make docker-facs-shell       # Open bash in container
make docker-db-up            # Start with live reload
make docker-db-stop          # Stop development mode
make docker-db-logs          # View development logs
make docker-db-shell         # Open bash in container
make docker-ps               # Show running containers
make docker-clean            # Stop and remove volumes
```

## Direct Docker Compose Usage

```bash
cd docker/compose

# Start PostgreSQL + FACS
docker-compose up -d

# Development with live reload
docker-compose -f docker-compose.dev.yml up -d
```

## Helper Script

Use the convenience script:

```bash
cd docker/scripts
./docker.sh build           # Build image
./docker.sh up              # Start services
./docker.sh logs            # View logs
./docker.sh shell           # Open shell
./docker.sh help            # Show all commands
```

## Directory Structure

```
docker/
├── Dockerfile                 # Production multi-stage image
├── compose/
│   ├── docker-compose.yml    # PostgreSQL production
│   └── docker-compose.dev.yml     # Development overrides
├── config/
│   └── .env.example          # Environment template
├── database/
│   └── init-db.sql          # PostgreSQL init script
├── scripts/
│   └── docker.sh            # Helper script
├── certs/                    # SSL certificates (optional)
└── README.md                 # This file
```

## Configuration

```bash
cp config/.env.example .env
```

**Key variables:**

```env
# PostgreSQL Database
DATABASE_URL=postgresql+asyncpg://facs:facs_password@postgres:5432/facs

# Server
FACS_IP=0.0.0.0
FACS_PORT=8000
FACS_LOG_LEVEL=INFO

# PostgreSQL Credentials
POSTGRES_USER=facs
POSTGRES_PASSWORD=facs_password
POSTGRES_DB=facs
```

**Using custom config:**

```bash
cd docker/compose
docker-compose up -d --env-file config/.env
```

## Data Backup

### PostgreSQL

```bash
# Backup database
docker exec facs-postgres pg_dump -U facs facs > backup.sql

# Restore database
docker exec -i facs-postgres psql -U facs facs < backup.sql

# Check backup
tail -20 backup.sql
```

## Troubleshooting

### PostgreSQL Connection Issues

```bash
# Check logs
cd docker/compose
docker-compose logs postgres

# Test connection
docker-compose exec postgres psql -U facs -d facs -c "SELECT 1"

# Reinitialize database (warning: deletes data)
docker-compose down -v
docker-compose up
```

### Port conflicts

Change port mappings:

```bash
docker-compose up -p 5433:5432  # PostgreSQL on 5433
docker-compose up -p 9000:8000  # FACS on 9000
```

## Security Notes

1. **Change default PostgreSQL password** in production:
   - Edit `docker-compose.yml` before first start
   - Set `POSTGRES_PASSWORD` to secure value

2. Keep `.env` file **out of version control**

3. Configure SSL certificates in `certs/` directory

4. Containers run as non-root user (already configured)

## For More Information

See [docs/DOCKER.md](../docs/DOCKER.md) for comprehensive Docker deployment documentation.
