#!/usr/bin/env python3
"""
Find and copy CIF files for a given CCD code from set1 and set2 directories.
Usage: python find_and_copy_cif.py <ccd_code> [set1_dir] [set2_dir]
"""
import os
import shutil
import sys

# Default directories
default_set1_dir = r'C:\Users\dharrus\Documents\Deborah-Portable\set1_files'
default_set2_dir = r'C:\Users\dharrus\Documents\Deborah-Portable\set2_files'

def find_cif_file(ccd_code: str, base_dir: str) -> str:
    """Find the CIF file for a given CCD code in the directory structure."""
    last_char = ccd_code[-1]
    
    # Try different possible paths
    possible_paths = [
        os.path.join(base_dir, last_char, ccd_code, f'{ccd_code}.cif'),
        os.path.join(base_dir, last_char, f'{ccd_code}.cif'),
        os.path.join(base_dir, ccd_code, f'{ccd_code}.cif'),
        os.path.join(base_dir, f'{ccd_code}.cif'),
    ]
    
    # Try direct paths first
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    # If not found, search recursively
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == f'{ccd_code}.cif':
                return os.path.join(root, file)
    
    return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python find_and_copy_cif.py <ccd_code> [set1_dir] [set2_dir]")
        print("\nExample:")
        print("  python find_and_copy_cif.py 2J0")
        print("  python find_and_copy_cif.py 2J0 C:\\path\\to\\set1 C:\\path\\to\\set2")
        sys.exit(1)
    
    ccd_code = sys.argv[1]
    set1_dir = sys.argv[2] if len(sys.argv) > 2 else default_set1_dir
    set2_dir = sys.argv[3] if len(sys.argv) > 3 else default_set2_dir
    
    print(f"Searching for CCD code: {ccd_code}")
    print(f"Set1 directory: {set1_dir}")
    print(f"Set2 directory: {set2_dir}")
    print()
    
    # Find files
    found1 = find_cif_file(ccd_code, set1_dir)
    found2 = find_cif_file(ccd_code, set2_dir)
    
    print(f'Set1 file: {found1 if found1 else "NOT FOUND"}')
    print(f'Set2 file: {found2 if found2 else "NOT FOUND"}')
    print()
    
    # Copy files if found
    if found1:
        dest1 = f'example_set1_{ccd_code}.cif'
        shutil.copy2(found1, dest1)
        print(f'✓ Copied set1 file to {dest1}')
    else:
        print('✗ Set1 file not found - cannot copy')
    
    if found2:
        dest2 = f'example_set2_{ccd_code}.cif'
        shutil.copy2(found2, dest2)
        print(f'✓ Copied set2 file to {dest2}')
    else:
        print('✗ Set2 file not found - cannot copy')

if __name__ == '__main__':
    main()

