"""
Shared context and helper functions for the MCP rosbag server.
This module avoids circular imports by providing a centralized location for shared state.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from typing import TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from .server import FastMCP

logger = logging.getLogger(__name__)

# Global MCP server instance.
# This will be initialized by server.py at runtime.
mcp: Optional["FastMCP"] = None
config: Dict[str, Any] = {}
schema_manager = None
cache_manager = None
typestore = None
CURRENT_BAG_PATH: Optional[Path] = None
BAG_DIRECTORY: Optional[Path] = None

def is_ros2_bag_dir(path: Path) -> bool:
    """Check if a directory is a ROS 2 bag directory.
    
    Args:
        path: Path to check
        
    Returns:
        True if the directory contains a metadata.yaml file (indicating a ROS 2 bag), False otherwise
    """
    return (path / 'metadata.yaml').exists()

def get_bag_files(custom_path: Optional[str] = None) -> List[Path]:
    """Get all rosbag files in the directory or from custom path.
    
    This function searches for ROS bag files in various formats:
    - Individual .mcap or .db3 files
    - ROS 2 bag directories (containing metadata.yaml)
    - Nested directories containing bag files
    
    Args:
        custom_path: Optional custom path to search. If not provided, uses CURRENT_BAG_PATH or BAG_DIRECTORY
        
    Returns:
        Sorted list of Path objects pointing to bag files or directories
    """
    from rosbags.rosbag2 import Reader
    
    bags = []
    
    # Determine the search path
    if custom_path:
        search_path = Path(custom_path)
    else:
        search_path = CURRENT_BAG_PATH or BAG_DIRECTORY
    
    if not search_path or not search_path.exists():
        logger.warning(f"Search path does not exist: {search_path}")
        return []
    
    # Handle single file
    if search_path.is_file() and search_path.suffix in ['.mcap', '.db3']:
        logger.debug(f"Found single bag file: {search_path}")
        return [search_path]
    
    # Handle directory cases
    if search_path.is_dir():
        # Check if this is a ROS 2 bag directory itself
        if is_ros2_bag_dir(search_path):
            logger.debug(f"Found ROS 2 bag directory: {search_path}")
            return [search_path]
        
        # Otherwise look for bags inside
        for item in search_path.iterdir():
            if item.is_dir() and is_ros2_bag_dir(item):
                # ROS 2 bag directory
                bags.append(item)
            elif item.is_file() and item.suffix in ['.mcap', '.db3']:
                # Individual bag files
                bags.append(item)
        
        # Also check subdirectories one level deep
        for subdir in search_path.iterdir():
            if subdir.is_dir() and not is_ros2_bag_dir(subdir):
                for item in subdir.iterdir():
                    if item.is_dir() and is_ros2_bag_dir(item):
                        bags.append(item)
    
    logger.debug(f"Found {len(bags)} bag files/directories")
    return sorted(bags)

def deserialize_message(rawdata: bytes, msg_type: str) -> Tuple[Any, Dict[str, Any]]:
    """Deserialize message data and apply schema extraction.
    
    This function takes raw CDR-serialized message data and converts it to a Python dictionary.
    It also applies schema extraction if a schema manager is configured, which extracts only
    relevant fields according to predefined schemas.
    
    Args:
        rawdata: Raw bytes from the bag file containing CDR-serialized message data
        msg_type: ROS message type string (e.g., 'sensor_msgs/msg/LaserScan')
        
    Returns:
        A tuple containing:
        - The deserialized message object
        - A dictionary representation (either full or schema-extracted)
    """
    from .core.message_utils import msg_to_dict
    
    msg = typestore.deserialize_cdr(rawdata, msg_type)
    msg_dict = msg_to_dict(msg, msg_type)
    
    # Apply schema extraction if available
    if schema_manager:
        extracted = schema_manager.extract_fields(msg_dict, msg_type)
        return msg, extracted
    else:
        return msg, msg_dict
