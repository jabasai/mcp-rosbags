# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files first for better caching
COPY pyproject.toml README.md .python-version* /app/



# Copy the application code
COPY mcp_rosbag_server/ ./mcp_rosbag_server/

# Install dependencies and the package
# Note: In environments with SSL interception (corporate proxies, some CI systems),
# you may need to configure CA certificates or use custom registry settings.
# uv respects UV_INDEX_URL and UV_EXTRA_INDEX_URL environment variables.
RUN uv pip install --system --no-cache -e .

# Set environment variables
ENV PYTHONPATH=/app/src
ENV MCP_ROSBAG_DIR=/rosbags
ENV MCP_ROSBAG_CONFIG=/app/src/config

# Create directories for rosbags and config
RUN mkdir -p /rosbags

# Expose the MCP server (stdio-based, no port needed but useful for documentation)
# The server communicates via stdin/stdout

CMD ["/usr/local/bin/mcp-rosbag-server"]

