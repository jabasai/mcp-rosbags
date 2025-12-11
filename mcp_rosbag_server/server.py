#!/usr/bin/env python3
"""
MCP server for rosbag memory capabilities using fastmcp with decorators.
"""

import asyncio
import os
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastmcp import FastMCP

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

# Import core utilities
from .core.schema_manager import SchemaManager
from .core.cache_manager import CacheManager

# Import shared module
from . import shared

import logging

# Setup logging
log_file = f"/tmp/mcp_rosbag_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global configuration
# Default to config directory relative to this source file
_DEFAULT_CONFIG_DIR = Path(__file__).parent / "config"
CONFIG_DIR = Path(os.environ.get("MCP_ROSBAG_CONFIG", str(_DEFAULT_CONFIG_DIR)))

# Create FastMCP server
mcp = FastMCP("rosbag-memory")

# Store mcp instance in shared module
shared.mcp = mcp

def initialize_components():
    """Initialize all components from config."""
    # Load configuration
    config_file = CONFIG_DIR / "server_config.yaml"
    if config_file.exists():
        with open(config_file, 'r') as f:
            shared.config = yaml.safe_load(f)
            logger.info(f"Loaded config from {config_file}")
    else:
        # Default configuration
        shared.config = {
            "data": {
                "time_tolerance": 0.1,
                "max_range_messages": 100,
                "max_array_length": 100,
                "position_tolerance": 1.0
            },
            "cache": {
                "enabled": True,
                "max_size": 100,
                "ttl_seconds": 300
            },
            "logging": {
                "default_limit": 100
            }
        }
        logger.warning(f"No config file found at {config_file}, using defaults")
    
    # Initialize typestore for deserialization
    shared.typestore = get_typestore(Stores.LATEST)
    logger.info("Initialized typestore for message deserialization")
    
    # Initialize schema manager
    schema_file = CONFIG_DIR / "message_schemas.yaml"
    shared.schema_manager = SchemaManager(schema_file, shared.config['data'].get('max_array_length', 100))
    
    # Initialize cache manager
    if shared.config['cache']['enabled']:
        shared.cache_manager = CacheManager(
            max_size=shared.config['cache']['max_size'],
            ttl_seconds=shared.config['cache']['ttl_seconds']
        )
        logger.info("Cache manager initialized")

# Core tools using fastmcp decorators
@mcp.tool()
async def set_bag_path(path: str) -> Dict[str, Any]:
    """Set the path to a rosbag file or directory containing rosbags.
    
    Args:
        path: Path to rosbag file or directory
    """
    logger.info(f"Setting bag path to: {path}")
    
    path_obj = Path(path)
    if not path_obj.exists():
        logger.error(f"Path does not exist: {path}")
        return {"error": f"Path does not exist: {path}"}
    
    if path_obj.is_file() and path_obj.suffix not in ['.mcap', '.db3']:
        logger.error(f"Invalid bag file: {path}")
        return {"error": f"File is not a rosbag: {path}"}
    
    shared.CURRENT_BAG_PATH = path_obj
    
    # Clear cache when changing bag path
    if shared.cache_manager:
        shared.cache_manager.clear()
    
    logger.info(f"Successfully set bag path to: {path}")
    
    # Return info about what was set
    if path_obj.is_file():
        return {
            "success": True,
            "type": "file",
            "path": str(path_obj),
            "size_mb": path_obj.stat().st_size / (1024 * 1024)
        }
    elif shared._is_ros2_bag_dir(path_obj):
        # This is a ROS 2 bag directory
        db3_files = list(path_obj.glob("*.db3"))
        mcap_files = list(path_obj.glob("*.mcap"))
        total_size = sum(f.stat().st_size for f in db3_files + mcap_files)
        return {
            "success": True,
            "type": "ros2_bag_directory",
            "path": str(path_obj),
            "db3_files": len(db3_files),
            "mcap_files": len(mcap_files),
            "total_size_mb": total_size / (1024 * 1024)
        }
    else:
        # Regular directory containing bags
        bag_files = shared.get_bag_files()
        return {
            "success": True,
            "type": "directory",
            "path": str(path_obj),
            "bag_count": len(bag_files),
            "bags": [str(f.name) for f in bag_files]
        }

@mcp.tool()
async def list_bags(path: Optional[str] = None) -> Dict[str, Any]:
    """List all available rosbag files in the current or specified directory.
    
    Args:
        path: Optional directory to search for bags (defaults to current bag path)
    """
    logger.info(f"Listing bags from {path or 'current path'}")
    
    bag_files = shared.get_bag_files(path)
    bags_info = []
    
    for bag_file in bag_files:
        try:
            # Get basic info for each bag
            bag_data = {
                "path": str(bag_file),
                "name": bag_file.name,
                "type": "directory" if bag_file.is_dir() else "file"
            }
            
            # Add size info
            if bag_file.is_file():
                bag_data["size_mb"] = bag_file.stat().st_size / (1024 * 1024)
            else:
                # ROS 2 bag directory
                total_size = 0
                for f in bag_file.glob("*.db3"):
                    total_size += f.stat().st_size
                for f in bag_file.glob("*.mcap"):
                    total_size += f.stat().st_size
                bag_data["size_mb"] = total_size / (1024 * 1024)
            
            # Try to get quick metadata without reading all messages
            try:
                with Reader(bag_file) as reader:
                    # Just get topic count and connection info
                    topics = [conn.topic for conn in reader.connections]
                    bag_data["topic_count"] = len(set(topics))
                    bag_data["topics"] = list(set(topics))[:10]  # First 10 topics as preview
            except Exception as e:
                logger.debug(f"Could not read metadata for {bag_file}: {e}")
                bag_data["topic_count"] = "unknown"
                bag_data["topics"] = []
            
            bags_info.append(bag_data)
            
        except Exception as e:
            logger.error(f"Error processing bag {bag_file}: {e}")
            continue
    
    logger.info(f"Found {len(bags_info)} bags")
    
    # Sort by name
    bags_info.sort(key=lambda x: x["name"])
    
    return {
        "bag_count": len(bags_info),
        "bags": bags_info,
        "current_path": str(shared.CURRENT_BAG_PATH) if shared.CURRENT_BAG_PATH else str(shared.BAG_DIRECTORY)
    }

@mcp.tool()
async def bag_info(bag_path: str) -> Dict[str, Any]:
    """Get detailed information about a rosbag including all topics, message counts, and duration.
    
    Args:
        bag_path: Path to rosbag file or directory
    """
    logger.info(f"Getting info for bag: {bag_path}")
    
    path = Path(bag_path)
    
    if not path.exists():
        logger.error(f"Path does not exist: {bag_path}")
        return {"error": f"Path does not exist: {bag_path}"}
    
    if not (path.is_file() and path.suffix in ['.mcap', '.db3']) and not shared._is_ros2_bag_dir(path):
        logger.error(f"Not a valid rosbag: {bag_path}")
        return {"error": f"Not a valid rosbag: {bag_path}"}
    
    info = {
        "path": str(path),
        "size_mb": 0,
        "duration": 0,
        "start_time": None,
        "end_time": None,
        "message_count": 0,
        "topics": {},  # Will contain ALL topics with full details
        "compression": None
    }
    
    # Calculate size
    if path.is_file():
        info["size_mb"] = path.stat().st_size / (1024 * 1024)
    else:
        # ROS 2 bag directory
        total_size = 0
        for f in path.glob("*.db3"):
            total_size += f.stat().st_size
        for f in path.glob("*.mcap"):
            total_size += f.stat().st_size
        info["size_mb"] = total_size / (1024 * 1024)
    
    try:
        with Reader(path) as reader:
            # Get compression info if available
            if hasattr(reader, 'compression_mode'):
                info["compression"] = reader.compression_mode
            
            # First pass: get topics and connections
            for conn in reader.connections:
                if conn.topic not in info["topics"]:
                    info["topics"][conn.topic] = {
                        "type": conn.msgtype,
                        "count": 0,
                        "frequency": 0.0,
                        "first_time": None,
                        "last_time": None
                    }
            
            # Second pass: count messages and get time range
            for conn, timestamp, _ in reader.messages():
                time_sec = timestamp / 1e9
                
                # Update global time range
                if info["start_time"] is None or time_sec < info["start_time"]:
                    info["start_time"] = time_sec
                if info["end_time"] is None or time_sec > info["end_time"]:
                    info["end_time"] = time_sec
                
                # Update topic stats
                topic_info = info["topics"][conn.topic]
                topic_info["count"] += 1
                info["message_count"] += 1
                
                # Update topic time range
                if topic_info["first_time"] is None or time_sec < topic_info["first_time"]:
                    topic_info["first_time"] = time_sec
                if topic_info["last_time"] is None or time_sec > topic_info["last_time"]:
                    topic_info["last_time"] = time_sec
            
            # Calculate duration and frequencies
            if info["start_time"] and info["end_time"]:
                info["duration"] = info["end_time"] - info["start_time"]
                
                # Calculate frequencies for each topic
                for topic, topic_info in info["topics"].items():
                    if topic_info["first_time"] and topic_info["last_time"]:
                        topic_duration = topic_info["last_time"] - topic_info["first_time"]
                        if topic_duration > 0:
                            topic_info["frequency"] = topic_info["count"] / topic_duration
            
            # Sort topics by message count (but keep ALL topics)
            info["topics"] = dict(sorted(info["topics"].items(), 
                                       key=lambda x: x[1]["count"], 
                                       reverse=True))
            
            # Add topic count for quick reference
            info["topic_count"] = len(info["topics"])
            
    except Exception as e:
        logger.error(f"Error reading bag: {e}", exc_info=True)
        return {"error": f"Error reading bag: {str(e)}"}
    
    # Format times nicely
    if info["start_time"]:
        info["start_time_human"] = datetime.fromtimestamp(info["start_time"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if info["end_time"]:
        info["end_time_human"] = datetime.fromtimestamp(info["end_time"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    logger.info(f"Bag info: {info['message_count']} messages, {info['duration']:.2f}s duration, {info['topic_count']} topics")
    
    return info

@mcp.tool()
async def get_message_at_time(
    topic: str,
    timestamp: float,
    tolerance: Optional[float] = None,
    bag_path: Optional[str] = None
) -> Dict[str, Any]:
    """Get the message from a topic at a specific time.
    
    Args:
        topic: ROS topic name
        timestamp: Unix timestamp in seconds
        tolerance: Time tolerance in seconds (default: from config)
        bag_path: Optional specific bag file or directory to search
    """
    if tolerance is None:
        tolerance = shared.config['data']['time_tolerance']
        
    logger.info(f"Searching for message on topic {topic} at {timestamp} with tolerance {tolerance}s")
    
    target_ns = int(timestamp * 1e9)  # Convert to nanoseconds
    tolerance_ns = int(tolerance * 1e9)
    
    closest_msg = None
    closest_time = None
    min_diff = float('inf')
    
    bag_files = shared.get_bag_files(bag_path)
    logger.debug(f"Searching {len(bag_files)} bag files")
    
    for bag_file in bag_files:
        try:
            with Reader(bag_file) as reader:
                logger.debug(f"Reading bag file: {bag_file}")
                # Check if topic exists in this bag
                if topic not in [conn.topic for conn in reader.connections]:
                    logger.debug(f"Topic {topic} not found in {bag_file}")
                    continue
                
                for conn, ts, rawdata in reader.messages():
                    if conn.topic != topic:
                        continue
                    
                    diff = abs(ts - target_ns)
                    if diff < min_diff and diff <= tolerance_ns:
                        min_diff = diff
                        closest_time = ts
                        # Deserialize the message with schema extraction
                        _, msg_dict = shared.deserialize_message(rawdata, conn.msgtype)
                        closest_msg = msg_dict
        except Exception as e:
            logger.error(f"Error reading {bag_file}: {e}")
            continue
    
    if closest_msg is None or closest_time is None:
        logger.warning(f"No message found for topic {topic} within {tolerance}s of timestamp {timestamp}")
        return {
            "error": f"No message found for topic {topic} within {tolerance}s of timestamp {timestamp}"
        }
    
    logger.info(f"Found message at {closest_time / 1e9} with time diff {min_diff / 1e9}s")
    
    return {
        "topic": topic,
        "timestamp": closest_time / 1e9,  # Convert back to seconds
        "time_diff": min_diff / 1e9,  # How far from requested time
        "message": closest_msg
    }

@mcp.tool()
async def get_messages_in_range(
    topic: str,
    start_time: float,
    end_time: float,
    max_messages: Optional[int] = None,
    bag_path: Optional[str] = None
) -> Dict[str, Any]:
    """Get all messages from a topic within a time range.
    
    Args:
        topic: ROS topic name
        start_time: Start unix timestamp in seconds
        end_time: End unix timestamp in seconds
        max_messages: Maximum messages to return (default: from config)
        bag_path: Optional specific bag file or directory to search
    """
    if max_messages is None:
        max_messages = shared.config['data']['max_range_messages']
        
    logger.info(f"Getting messages for topic {topic} between {start_time} and {end_time}")
    
    start_ns = int(start_time * 1e9)
    end_ns = int(end_time * 1e9)
    
    messages = []
    
    for bag_file in shared.get_bag_files(bag_path):
        try:
            with Reader(bag_file) as reader:
                # Check if topic exists
                if topic not in [conn.topic for conn in reader.connections]:
                    continue
                
                for conn, timestamp, rawdata in reader.messages():
                    if conn.topic != topic:
                        continue
                    
                    if start_ns <= timestamp <= end_ns:
                        _, msg_dict = shared.deserialize_message(rawdata, conn.msgtype)
                        messages.append({
                            "timestamp": timestamp / 1e9,
                            "message": msg_dict
                        })
                        
                        if len(messages) >= max_messages:
                            break
            
            if len(messages) >= max_messages:
                break
        except Exception as e:
            logger.error(f"Error reading {bag_file}: {e}")
            continue
    
    logger.info(f"Found {len(messages)} messages in range")
    
    return {
        "topic": topic,
        "start_time": start_time,
        "end_time": end_time,
        "message_count": len(messages),
        "messages": messages
    }

# Import extractor modules which will register their tools via decorators
def register_extractor_tools():
    """Import all extractor modules which will register their tools via decorators."""
    try:
        from .extractors import (
            bag_management,
            trajectory,
            logging,
            search,
            visualization,
            lidar,
            tf_tree,
            image
        )
        logger.info("Registered all extractor tools")
    except Exception as e:
        logger.error(f"Error registering extractor tools: {e}", exc_info=True)

async def async_main():
    """Run the MCP server."""
    import argparse
    
    # Default to config directory relative to this source file
    default_config_dir = Path(__file__).parent / "config"
    
    parser = argparse.ArgumentParser(description="MCP server for rosbag memory")
    parser.add_argument("--bag-dir", default="./rosbags", help="Directory containing rosbag files")
    parser.add_argument("--config-dir", default=str(default_config_dir), help="Directory containing configuration files")
    args = parser.parse_args()
    
    shared.BAG_DIRECTORY = Path(args.bag_dir)
    CONFIG_DIR_LOCAL = Path(args.config_dir)
    
    # Update the global CONFIG_DIR
    global CONFIG_DIR
    CONFIG_DIR = CONFIG_DIR_LOCAL
    
    logger.info(f"Starting MCP rosbag server")
    logger.info(f"Bag directory: {shared.BAG_DIRECTORY}")
    logger.info(f"Config directory: {CONFIG_DIR}")
    logger.info(f"Log file: {log_file}")
    
    # Initialize components
    initialize_components()
    
    # Register extractor tools
    register_extractor_tools()
    
    try:
        # Run the FastMCP server
        await mcp.run_stdio_async()
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)

def main():
    """Entry point for the MCP server."""
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
