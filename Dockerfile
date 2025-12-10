# Use ROS 2 Humble base image
FROM ros:humble-ros-base

# Set working directory
WORKDIR /app

# Install Python dependencies and additional tools
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies (with trusted host for SSL issues in some environments)
RUN pip3 install --no-cache-dir -r requirements.txt || \
    pip3 install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Copy the application code
COPY src/ ./src/
COPY setup.py .

# Install the package
RUN pip3 install -e .

# Set environment variables
ENV PYTHONPATH=/app/src
ENV MCP_ROSBAG_DIR=/rosbags
ENV MCP_ROSBAG_CONFIG=/app/src/config

# Create directories for rosbags and config
RUN mkdir -p /rosbags

# Expose the MCP server (stdio-based, no port needed but useful for documentation)
# The server communicates via stdin/stdout

# Set the entrypoint to run the MCP server
ENTRYPOINT ["python3", "/app/src/server.py"]

# Default command arguments (can be overridden)
CMD []
