#!/usr/bin/env python3
"""
Complete refactoring script to convert all extractor modules to use fastmcp decorators.
"""

import re
from pathlib import Path

def refactor_extractor_file(file_path: Path) -> str:
    """Refactor a single extractor file to use fastmcp decorators."""
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Remove the Tool import
    content = re.sub(r'from mcp\.types import Tool\n', '', content)
    
    # Add import for mcp instance from shared
    if "from ..shared import mcp" not in content:
        # Find the right place to add the import (after other imports)
        import_section_end = 0
        for match in re.finditer(r'^(from |import )', content, re.MULTILINE):
            import_section_end = match.end()
            # Find end of this line
            newline_pos = content.find('\n', import_section_end)
            if newline_pos != -1:
                import_section_end = newline_pos + 1
        
        # Insert the new import
        new_import = '\n# Import mcp instance from shared module\nfrom ..shared import mcp, get_bag_files, deserialize_message, config\n'
        content = content[:import_section_end] + new_import + content[import_section_end:]
    
    # Find all async function definitions that have the pattern we're looking for
    # These are tool handler functions that take _get_bag_files_fn, _deserialize_message_fn, config
    pattern = r'async def (\w+)\((.*?)\) -> Dict\[str, Any\]:'
    
    def replace_function(match):
        func_name = match.group(1)
        params = match.group(2)
        
        # Remove the internal parameters (_get_bag_files_fn, _deserialize_message_fn, config, bag_path)
        # These should be accessed via shared module now
        params_list = [p.strip() for p in params.split(',')]
        new_params = []
        
        for param in params_list:
            # Skip internal function parameters
            if any(skip in param for skip in ['_get_bag_files_fn', '_deserialize_message_fn', 'config: Dict']):
                continue
            new_params.append(param)
        
        new_params_str = ', '.join(new_params)
        
        # Add the decorator
        return f'@mcp.tool()\nasync def {func_name}({new_params_str}) -> Dict[str, Any]:'
    
    content = re.sub(pattern, replace_function, content)
    
    # Remove the register_*_tools function entirely
    register_pattern = r'\n\ndef register_\w+_tools\(.*?\n.*?return tools, \{.*?\}'
    content = re.sub(register_pattern, '', content, flags=re.DOTALL)
    
    # Replace function calls to _get_bag_files_fn with get_bag_files
    content = content.replace('_get_bag_files_fn(', 'get_bag_files(')
    content = content.replace('_get_bag_files_fn', 'get_bag_files')
    
    # Replace function calls to _deserialize_message_fn with deserialize_message
    content = content.replace('_deserialize_message_fn(', 'deserialize_message(')
    content = content.replace('_deserialize_message_fn', 'deserialize_message')
    
    return content

def main():
    """Main refactoring function."""
    extractors_dir = Path('mcp_rosbag_server/extractors')
    
    files_to_refactor = [
        'bag_management.py',
        'trajectory.py',
        'logging.py',
        'search.py',
        'visualization.py',
        'lidar.py',
        'tf_tree.py',
        'image.py'
    ]
    
    for filename in files_to_refactor:
        file_path = extractors_dir / filename
        if not file_path.exists():
            print(f"Skipping {filename} - not found")
            continue
        
        print(f"Refactoring {filename}...")
        try:
            refactored_content = refactor_extractor_file(file_path)
            
            # Write to a new file first to check
            output_path = extractors_dir / f"{file_path.stem}_refactored.py"
            with open(output_path, 'w') as f:
                f.write(refactored_content)
            
            print(f"  ✓ Created {output_path}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

if __name__ == '__main__':
    main()
