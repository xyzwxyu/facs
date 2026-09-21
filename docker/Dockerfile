# FACS v0.1.0
# Multi-stage build for FACS
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir .

# Runtime stage
FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/facs /usr/local/bin/facs

# Copy application for database volume
COPY . .

# Create non-root user
RUN useradd -m -u 1000 facs && chown -R facs:facs /app
USER facs

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD facs --help > /dev/null 2>&1 || exit 1

# Expose port
EXPOSE 8000

# Default command
CMD ["facs", "--ip", "0.0.0.0", "--port", "8000"]
