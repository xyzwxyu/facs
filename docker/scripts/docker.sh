#!/bin/bash

# FACS Docker Management Script
# Usage: ./docker.sh [command] [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
COMPOSE_DIR="$(dirname "$SCRIPT_DIR")/compose"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"
DEV_COMPOSE_FILE="$COMPOSE_DIR/docker-compose.dev.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
print_help() {
    cat << EOF
FACS Docker Management Script

Usage: ./docker.sh [command] [options]

Commands:
  build [TAG]         Build Docker image (default tag: latest)
  up                  Start services in background
  down                Stop services
  logs [SERVICE]      View service logs (default: facs)
  ps                  Show running containers
  shell               Open shell in facs container
  cleanup             Remove stopped containers and dangling images
  dev [up|down|logs]  Development mode commands
  help                Show this help message

Examples:
  ./docker.sh build                    # Build image with tag 'latest'
  ./docker.sh build 0.1.0              # Build image with tag '0.1.0'
  ./docker.sh up                       # Start services
  ./docker.sh logs                     # Follow facs logs
  ./docker.sh logs nginx               # Follow nginx logs
  ./docker.sh shell                    # Open shell in container
  ./docker.sh dev up                   # Start in development mode
  ./docker.sh cleanup                  # Clean up unused resources

EOF
}

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Commands
build_image() {
    local tag="${1:-latest}"
    print_info "Building Docker image: facs:$tag"
    docker build \
        -f "$PROJECT_ROOT/docker/Dockerfile" \
        -t "facs:$tag" \
        "$PROJECT_ROOT"
    print_status "Image built successfully: facs:$tag"
}

start_services() {
    print_info "Starting services..."
    cd "$COMPOSE_DIR"
    docker-compose -f "$COMPOSE_FILE" up -d
    print_status "Services started"
    print_info "Run './docker.sh logs' to view logs"
}

stop_services() {
    print_info "Stopping services..."
    cd "$COMPOSE_DIR"
    docker-compose -f "$COMPOSE_FILE" down
    print_status "Services stopped"
}

view_logs() {
    local service="${1:-facs}"
    print_info "Following logs for: $service"
    cd "$COMPOSE_DIR"
    docker-compose -f "$COMPOSE_FILE" logs -f "$service"
}

show_status() {
    cd "$COMPOSE_DIR"
    docker-compose -f "$COMPOSE_FILE" ps
}

open_shell() {
    print_info "Opening shell in facs container..."
    cd "$COMPOSE_DIR"
    docker-compose -f "$COMPOSE_FILE" exec facs /bin/bash
}

cleanup() {
    print_info "Cleaning up stopped containers and dangling images..."
    docker container prune -f
    docker image prune -f
    print_status "Cleanup completed"
}

dev_mode() {
    local cmd="${1:-up}"
    case "$cmd" in
        up)
            print_info "Starting development environment..."
            cd "$COMPOSE_DIR"
            docker-compose -f "$COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" up -d
            print_status "Development environment started"
            ;;
        down)
            print_info "Stopping development environment..."
            cd "$COMPOSE_DIR"
            docker-compose -f "$COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" down
            print_status "Development environment stopped"
            ;;
        logs)
            cd "$COMPOSE_DIR"
            docker-compose -f "$COMPOSE_FILE" -f "$DEV_COMPOSE_FILE" logs -f facs
            ;;
        *)
            print_error "Unknown dev command: $cmd"
            echo "Available commands: up, down, logs"
            exit 1
            ;;
    esac
}

# Main logic
case "${1:-help}" in
    build)
        build_image "$2"
        ;;
    up)
        build_image latest
        start_services
        ;;
    down)
        stop_services
        ;;
    logs)
        view_logs "$2"
        ;;
    ps)
        show_status
        ;;
    shell)
        open_shell
        ;;
    cleanup)
        cleanup
        ;;
    dev)
        dev_mode "$2"
        ;;
    help)
        print_help
        ;;
    *)
        print_error "Unknown command: $1"
        print_help
        exit 1
        ;;
esac
