# MCP ROS Bags

A Model Context Protocol (MCP) server for analyzing and querying ROS bag files. This server enables Claude and other MCP-compatible tools to interact with robot data stored in ROS bags.

## Overview

This MCP server provides comprehensive tools for ROS bag analysis, supporting both ROS 1 and ROS 2 bag formats (`.bag`, `.db3`, `.mcap`). It enables AI assistants to query robot trajectories, sensor data, transforms, and logging information from recorded robot sessions.

### Key Features

- **Bag Management**: List, inspect, and query ROS bag files
- **Message Querying**: Get messages by topic, timestamp, or time range
- **Trajectory Analysis**: Extract robot paths, waypoints, and motion profiles
- **Sensor Data**: Analyze laserscan topics, images
- **Transform Trees**: Query coordinate transforms and frame relationships
- **Logging Analysis**: Search and analyze ROS log messages
- **Visualization**: Generate plots and visual representations of data
- **Search & Discovery**: Find objects, patterns, and correlations in data

## Installation

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver.

#### Quick Start with uv tool

The fastest way to run the server:

1. **Install uv** (if not already installed):
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2. **Clone the repository:**
    ```bash
    git clone https://github.com/binabik-ai/mcp-rosbags.git
    cd mcp-rosbags
    ```

3. **Run directly as a tool (no installation needed):**
    ```bash
    uv tool run --from . mcp-rosbag-server
    ```

4. **Or install as a global tool:**
    ```bash
    uv tool install .
    ```
    
    Then run from anywhere:
    ```bash
    mcp-rosbag-server
    ```

#### Development Installation

For development or editable installs:

1. **Install dependencies:**
    ```bash
    uv pip install -e .
    ```


2. **Optional: Install development dependencies:**
    ```bash
    uv pip install -e .[dev]
    ```

### Using pip (Traditional)

1. **Clone the repository:**
    ```bash
    git clone https://github.com/binabik-ai/mcp-rosbags.git
    cd mcp-rosbags
    ```

2. **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. **Install dependencies:**
    ```bash
    pip install -e .
    ```

## Docker Deployment

### Building the Docker Image

Build the Docker image using the provided Dockerfile:

```bash
docker build -t mcp-rosbags:latest .
```

### Running with Docker Compose

The easiest way to run the MCP server in a container is using Docker Compose:

1. **Place your ROS bag files** in the `./rosbags` directory (or modify the volume mount in `compose.yaml`)

2. **Start the container:**
    ```bash
    docker compose up -d
    ```

3. **View logs:**
    ```bash
    docker compose logs -f
    ```

4. **Stop the container:**
    ```bash
    docker compose down
    ```

### Running with Docker

You can also run the container directly:

```bash
docker run -it --rm \
  -v /path/to/your/rosbags:/rosbags:ro \
  -v $(pwd)/src/config:/app/src/config:ro \
  mcp-rosbags:latest
```

### Docker Configuration

The Docker container:
- Uses **Python 3.11 slim** as the base image with uv for fast dependency installation
- Mounts your ROS bag directory to `/rosbags` (read-only)
- Mounts configuration files to `/app/src/config` (read-only)
- Writes logs to `/tmp/mcp_rosbag_*.log` inside the container
  - When using Docker Compose, these are accessible on the host at `./logs/mcp_rosbag_*.log`
- Communicates via stdin/stdout (MCP protocol)

## Configuration

### Claude Desktop Configuration

Add this to your Claude Desktop configuration file:

**Recommended: Using uv tool (after `uv tool install .`):**

```json
{
  "mcpServers": {
    "rosbag_reader": {
      "command": "mcp-rosbag-server",
      "env": {
        "MCP_ROSBAG_DIR": "/path/to/your/rosbags"
      }
    }
  }
}
```

**Alternative: Using uv tool run (no installation required):**

```json
{
  "mcpServers": {
    "rosbag_reader": {
      "command": "uv",
      "args": ["tool", "run", "--from", "/path/to/mcp-rosbags", "mcp-rosbag-server"],
      "env": {
        "MCP_ROSBAG_DIR": "/path/to/your/rosbags"
      }
    }
  }
}
```

**Alternative: Using virtual environment:**

```json
{
  "mcpServers": {
    "rosbag_reader": {
      "command": "/path/to/your/venv/bin/mcp-rosbag-server",
      "env": {
        "MCP_ROSBAG_DIR": "/path/to/your/rosbags"
      }
    }
  }
}
```

### Environment Variables

- `MCP_ROSBAG_DIR`: Default directory containing ROS bag files (default: `./rosbags`)
- `MCP_ROSBAG_CONFIG`: Configuration directory (default: `./config`)

### Server Configuration

The server uses configuration files in the `src/config/` directory:

- `server_config.yaml`: Main server configuration (caching, timeouts, etc.)
- `message_schemas.yaml`: Schema definitions for message field extraction

## Usage

### With Claude Desktop

After configuration, restart Claude Desktop and you can interact with your ROS bags:

```
"List all ROS bags in my directory"
"Show me the trajectory from the /odom topic"
"Find all error messages in the logs"
"Extract an image from the /camera/image topic at timestamp 1234567890 and describe what you see"
```

## Tools Overview

The server provides the following categories of tools:

### Core Tools
- `set_bag_path`: Set current bag file or directory
- `list_bags`: List available ROS bag files
- `bag_info`: Get detailed bag information
- `get_message_at_time`: Get message at specific timestamp
- `get_messages_in_range`: Get messages within time range

### Analysis Tools
- **Trajectory**: Extract robot paths, analyze motion patterns
- **LiDAR**: Process laserscan data, e.g., to detect obstacles
- **Images**: Extract and process camera images
- **Transforms**: Query coordinate transforms and frame trees
- **Logging**: Search and analyze ROS log messages
- **Visualization**: Generate plots and visual data representations

### Search Tools
- Pattern matching in message data
- Correlation analysis between topics
- Time-based data alignment

## Architecture

```
src/
├── server.py              # Main MCP server
├── config/                # Configuration files
│   ├── server_config.yaml # Server settings
│   └── message_schemas.yaml # Message field schemas
├── core/                  # Core utilities
│   ├── cache_manager.py   # Caching system
│   ├── message_utils.py   # Message processing
│   └── schema_manager.py  # Schema handling
└── extractors/           # Specialized analysis tools
    ├── bag_management.py # Bag operations
    ├── image.py          # Image processing
    ├── lidar.py          # LiDAR analysis
    ├── logging.py        # Log analysis
    ├── search.py         # Search and correlation
    ├── tf_tree.py        # Transform trees
    ├── trajectory.py     # Path analysis
    └── visualization.py  # Data visualization
```

## Supported Message Types

The server supports standard ROS message types including:
- **Navigation**: `nav_msgs/Odometry`, `nav_msgs/Path`
- **Geometry**: `geometry_msgs/PoseStamped`, `geometry_msgs/Twist`
- **Sensors**: `sensor_msgs/LaserScan`, `sensor_msgs/PointCloud2`, `sensor_msgs/Image`
- **Transforms**: `tf2_msgs/TFMessage`, `geometry_msgs/TransformStamped`
- **Diagnostics**: `diagnostic_msgs/DiagnosticArray`
- **Logging**: `rcl_interfaces/Log`

## Performance Features

- **Intelligent Caching**: Configurable caching system for frequently accessed data
- **Schema-based Extraction**: Efficient field extraction using predefined schemas

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

Built with the [Model Context Protocol](https://modelcontextprotocol.io/) and the [rosbags](https://github.com/ternaris/rosbags) library.
