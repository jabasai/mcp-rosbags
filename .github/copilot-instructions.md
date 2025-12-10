# GitHub Copilot Instructions for MCP ROS Bags

## Repository Overview

This is a Model Context Protocol (MCP) server that enables AI assistants like Claude to analyze and query ROS bag files. It supports both ROS 1 and ROS 2 bag formats (`.bag`, `.db3`, `.mcap`).

## Architecture

### Core Components

- **`src/server.py`**: Main MCP server implementing the Model Context Protocol
  - Handles tool registration and execution
  - Manages bag file paths and reading operations
  - Implements caching for performance optimization
  - Uses async/await for non-blocking operations

- **`src/core/`**: Core utility modules
  - `message_utils.py`: Message serialization/deserialization helpers
  - `schema_manager.py`: Schema-based field extraction from messages
  - `cache_manager.py`: LRU cache with TTL for message data

- **`src/extractors/`**: Specialized analysis tools (each registers its own MCP tools)
  - `bag_management.py`: Bag file operations
  - `trajectory.py`: Robot path and motion analysis
  - `lidar.py`: LaserScan data processing
  - `image.py`: Image extraction and processing
  - `tf_tree.py`: Transform tree queries
  - `logging.py`: ROS log message analysis
  - `visualization.py`: Data plotting and visualization
  - `search.py`: Pattern matching and correlation

### Configuration

- **`src/config/server_config.yaml`**: Server settings (cache, timeouts, limits)
- **`src/config/message_schemas.yaml`**: Schema definitions for message field extraction

## Key Design Patterns

1. **Tool Registration Pattern**: Each extractor module exports a `register_*_tools()` function that returns (tools, handlers)
2. **Bag Reading**: Uses `rosbags.rosbag2.Reader` for cross-format bag reading
3. **Message Deserialization**: `deserialize_cdr()` converts raw bytes to Python objects
4. **Schema Extraction**: SchemaManager applies configured schemas to extract relevant fields

## Dependencies

- **Core**: `mcp`, `rosbags`, `numpy`
- **Extras**: `pillow` (images), `plotly` (visualization), `pyyaml` (config)
- **Optional**: `rclpy`, `rosidl_runtime_py` (ROS 2 native support)

## Environment Variables

- `MCP_ROSBAG_DIR`: Default directory for bag files (default: `./rosbags`)
- `MCP_ROSBAG_CONFIG`: Configuration directory (default: `./config`)

## Code Style Guidelines

1. **Async Functions**: All tool handlers should be async
2. **Error Handling**: Log errors with context, return JSON error objects
3. **Logging**: Use the global logger with appropriate levels (DEBUG, INFO, WARNING, ERROR)
4. **Type Hints**: Use type hints for function parameters and returns
5. **Documentation**: Docstrings for all public functions and classes

## Testing Considerations

- No formal test suite exists yet
- Manual testing requires ROS bag files
- Test with both ROS 1 (.bag) and ROS 2 (.db3, .mcap) formats
- Verify tool registration and execution through MCP protocol

## Container Deployment

- Base image: ROS 2 Humble for full ROS 2 support
- Requires Python 3.8+ (ROS 2 Humble uses Python 3.10)
- Mount bag files as volumes
- Configuration can be overridden via environment variables

## Common Development Tasks

### Adding a New Tool

1. Create tool handler function in appropriate extractor module
2. Add tool definition with inputSchema
3. Register in the module's `register_*_tools()` function
4. Return updated tools list and handlers dict

### Modifying Message Processing

1. Update `message_schemas.yaml` for schema-based extraction
2. Modify `core/message_utils.py` for general message handling
3. Update individual extractors for specialized processing

### Performance Optimization

1. Use cache_manager for expensive operations
2. Limit message counts with max_messages parameters
3. Use schema extraction to reduce data size
4. Consider adding indices for large bag files

## Important Notes

- The server logs to `/tmp/mcp_rosbag_TIMESTAMP.log` by default
  - In Docker containers using compose.yaml, logs are accessible on the host in `./logs/`
- Cache is cleared when bag path changes
- Timestamp handling: ROS uses nanoseconds, convert to/from seconds for API
- Tool names must be unique across all extractors
