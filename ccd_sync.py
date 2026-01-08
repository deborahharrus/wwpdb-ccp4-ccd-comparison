#!/usr/bin/env python3
"""
Script to compare mmCIF files from two sources:
- Set 1: HTTP at https://files.wwpdb.org/pub/pdb/data/monomers/components.cif.gz (downloaded and split)
- Set 2: GitHub at https://github.com/MonomerLibrary/monomers/tree/master/

Optional dependency: tqdm (for progress bars)
    Install with: pip install tqdm
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, Request
import json
from multiprocessing import Pool, cpu_count
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    # Fallback if tqdm is not installed
    TQDM_AVAILABLE = False
    def tqdm(iterable, desc=None, total=None, unit=None, **kwargs):
        if desc:
            print(f"{desc}...")
        return iterable


class mmCIFParser:
    """Parser for mmCIF files."""
    
    def __init__(self, file_path: str = None, content: str = None):
        """Initialize parser with either a file path or content string.
        
        Args:
            file_path: Path to mmCIF file (if content is None)
            content: mmCIF file content as string (if file_path is None)
        """
        self.file_path = file_path
        self.data = {}
        self.loops = {}
        self._parse(content)
    
    def _parse(self, content: Optional[str] = None):
        """Parse the mmCIF file."""
        if content is not None:
            # Parse from content string
            lines = content.splitlines(keepends=True)
        else:
            # Parse from file
            with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        
        i = 0
        in_multiline = False
        multiline_key = None
        multiline_value = []
        
        while i < len(lines):
            line = lines[i].rstrip('\n\r')
            original_line = line
            line = line.strip()
            
            # Handle multi-line values (semicolon blocks)
            if line.startswith(';') and not in_multiline:
                # Start of multiline value
                in_multiline = True
                # Get the key from previous line
                if i > 0:
                    prev_line = lines[i-1].strip()
                    match = re.match(r'^_(\S+)\s*$', prev_line)
                    if match:
                        multiline_key = match.group(1)
                        multiline_value = []
                        # Capture content after semicolon on the same line
                        if len(line) > 1:
                            content_after_semicolon = line[1:].strip()
                            if content_after_semicolon:
                                multiline_value.append(content_after_semicolon)
                i += 1
                continue
            elif in_multiline:
                if line == ';':
                    # End of multiline value
                    if multiline_key:
                        self.data[multiline_key] = '\n'.join(multiline_value)
                    in_multiline = False
                    multiline_key = None
                    multiline_value = []
                else:
                    multiline_value.append(line)
                i += 1
                continue
            
            if not line or line.startswith('#'):
                i += 1
                continue
            
            # Parse single-value items (non-loop)
            # Pattern: _key followed by whitespace and value
            if line.startswith('_'):
                # Split on whitespace, but keep the value together
                parts = line.split(None, 1)  # Split on whitespace, max 1 split
                if len(parts) == 2:
                    key = parts[0]
                    value = parts[1].strip()
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    # Handle '?' as empty
                    if value == '?':
                        value = ''
                    self.data[key] = value
                i += 1
                continue
            
            # Parse loop_ blocks
            if line == 'loop_':
                i += 1
                # Read headers
                headers = []
                while i < len(lines):
                    header_line = lines[i].strip()
                    if not header_line or header_line.startswith('#'):
                        i += 1
                        continue
                    if header_line.startswith('_'):
                        headers.append(header_line)
                        i += 1
                    else:
                        break
                
                if not headers:
                    continue
                
                # Read data rows
                rows = []
                while i < len(lines):
                    data_line = lines[i].strip()
                    if not data_line or data_line.startswith('#'):
                        i += 1
                        continue
                    if data_line == 'loop_' or (data_line.startswith('_') and ' ' not in data_line and '\t' not in data_line):
                        # Next loop or single item (header without value)
                        break
                    
                    # Split the line - CIF format uses whitespace separation
                    # But we need to handle quoted values
                    values = self._split_cif_line(data_line)
                    if len(values) >= len(headers):
                        # Take only the number of values matching headers
                        rows.append(values[:len(headers)])
                    elif len(values) > 0:
                        # Partial row, pad with empty strings
                        while len(values) < len(headers):
                            values.append('')
                        rows.append(values)
                    i += 1
                
                if rows:
                    # Store as list of dicts
                    loop_data = []
                    for row in rows:
                        row_dict = {}
                        for j, header in enumerate(headers):
                            value = row[j] if j < len(row) else ''
                            # Remove quotes
                            if value.startswith('"') and value.endswith('"'):
                                value = value[1:-1]
                            elif value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                            row_dict[header] = value
                        loop_data.append(row_dict)
                    
                    # Store under first header's category
                    category = headers[0].split('.')[0]
                    self.loops[category] = {
                        'headers': headers,
                        'data': loop_data
                    }
                continue
            
            i += 1
    
    def _split_cif_line(self, line: str) -> List[str]:
        """Split a CIF line handling quoted values and multiple spaces."""
        values = []
        current = ''
        in_quotes = False
        quote_char = None
        
        i = 0
        while i < len(line):
            char = line[i]
            
            if char in ['"', "'"]:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                    current += char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                    current += char
                else:
                    current += char
            elif char.isspace() and not in_quotes:
                if current:
                    values.append(current)
                    current = ''
                # Skip multiple spaces
                while i + 1 < len(line) and line[i + 1].isspace():
                    i += 1
            else:
                current += char
            
            i += 1
        
        if current:
            values.append(current)
        
        return values
    
    def get_value(self, key: str) -> Optional[str]:
        """Get a single value by key."""
        return self.data.get(key)
    
    def get_loop_data(self, category: str) -> List[Dict[str, str]]:
        """Get loop data for a category."""
        # Try with and without underscore prefix
        if category in self.loops:
            return self.loops[category].get('data', [])
        elif f'_{category}' in self.loops:
            return self.loops[f'_{category}'].get('data', [])
        return []
    
    def get_loop_headers(self, category: str) -> List[str]:
        """Get loop headers for a category."""
        # Try with and without underscore prefix
        if category in self.loops:
            return self.loops[category].get('headers', [])
        elif f'_{category}' in self.loops:
            return self.loops[f'_{category}'].get('headers', [])
        return []


class FileDownloader:
    """Handle downloading files from HTTP/HTTPS and GitHub."""
    
    @staticmethod
    def download_and_split_components(show_progress: bool = True, output_dir: str = None) -> List[str]:
        """Download components.cif.gz, extract it, and split into individual CCD files.
        
        Downloads the gzipped components file from wwpdb.org, extracts it, and splits it 
        into individual CCD files in the proper directory structure.
        
        Args:
            show_progress: Whether to show progress messages
            output_dir: Directory where individual CCD files should be saved
        
        Returns:
            List of file paths (relative to output_dir) for each CCD file
        """
        import gzip
        import shutil
        
        # URL for the gzipped components file
        components_gz_url = "https://files.wwpdb.org/pub/pdb/data/monomers/components.cif.gz"
        components_cif_name = "Components-rel-alt.cif"
        
        if output_dir is None:
            output_dir = "set1_files"
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Paths for downloaded and extracted files
        gz_path = os.path.join(output_dir, "components.cif.gz")
        cif_path = os.path.join(output_dir, components_cif_name)
        
        # Check if we already have split files (resume support)
        # Need to recursively find all .cif files to check for existing ones
        existing_files = set()
        if os.path.exists(output_dir):
            for root, dirs, files in os.walk(output_dir):
                for f in files:
                    if f.endswith('.cif') and f != components_cif_name:
                        rel_path = os.path.relpath(os.path.join(root, f), output_dir)
                        # Normalize path separators for cross-platform compatibility
                        rel_path = rel_path.replace('\\', '/')
                        existing_files.add(rel_path)
        
        # If we have a reasonable number of existing files, assume we're done
        # (typical CCD count is ~30,000+)
        if len(existing_files) > 1000:
            if show_progress:
                print(f"Found {len(existing_files)} existing CCD files. Skipping download.")
            return sorted([f for f in existing_files])
        
        if show_progress:
            print("Downloading components.cif.gz from wwpdb.org...")
        
        # Download the gzipped file (skip if already exists)
        try:
            skip_download = os.path.exists(gz_path) and os.path.exists(cif_path)
            if not skip_download:
                req = Request(components_gz_url)
                req.add_header('User-Agent', 'Mozilla/5.0')
                with urlopen(req, timeout=300) as response:  # Large file, longer timeout
                    total_size = int(response.headers.get('Content-Length', 0))
                    if show_progress:
                        print(f"  File size: {total_size / (1024*1024):.1f} MB")
                    
                    with open(gz_path, 'wb') as f:
                        if show_progress and total_size > 0:
                            downloaded = 0
                            chunk_size = 8192
                            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                                while True:
                                    chunk = response.read(chunk_size)
                                    if not chunk:
                                        break
                                    f.write(chunk)
                                    downloaded += len(chunk)
                                    pbar.update(len(chunk))
                        else:
                            shutil.copyfileobj(response, f)
                
                if show_progress:
                    print("  Download complete. Extracting...")
                
                # Extract the gzipped file
                with gzip.open(gz_path, 'rb') as f_in:
                    with open(cif_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            elif skip_download and show_progress:
                print("  Using existing downloaded files.")
            
            if not os.path.exists(cif_path):
                if show_progress:
                    print("  Extracting...")
                # Extract the gzipped file
                with gzip.open(gz_path, 'rb') as f_in:
                    with open(cif_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            
            if show_progress:
                print("  Extraction complete. Splitting into individual CCD files...")
            
            if existing_files and show_progress:
                print(f"  Found {len(existing_files)} existing CCD files. Will only create missing ones...")
            
            def get_file_path(code: str) -> str:
                """Get the file path based on code length.
                
                For 3-char (or less): {last_char}/{first_two}/{code}.cif
                For 5-char: {last_char}/{code}/{code}.cif
                """
                code_len = len(code)
                if code_len <= 3:
                    # 3-char or less: {last_char}/{first_two}/{code}.cif
                    last_char = code[-1] if code_len > 0 else '0'
                    first_two = code[:-1] if code_len > 1 else '00'
                    # Pad first_two to 2 characters if needed
                    if len(first_two) == 0:
                        first_two = '00'
                    elif len(first_two) == 1:
                        first_two = '0' + first_two
                    return f"{last_char}/{first_two}/{code}.cif"
                elif code_len == 5:
                    # 5-char: {last_char}/{code}/{code}.cif
                    last_char = code[-1]
                    return f"{last_char}/{code}/{code}.cif"
                else:
                    # Fallback for other lengths: just use the code
                    return f"{code}.cif"
            
            # Parse and split the file
            file_list = []
            current_code = None
            current_lines = []
            files_to_create = []
            
            with open(cif_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    # Check if this is a data_ line (start of a new CCD)
                    if line.startswith('data_'):
                        # Save previous CCD if we have one
                        if current_code is not None and current_lines:
                            file_path = get_file_path(current_code)
                            file_list.append(file_path)
                            # Only write if file doesn't exist (resume support)
                            if file_path not in existing_files:
                                files_to_create.append((file_path, current_lines))
                            elif show_progress and len(file_list) % 1000 == 0:
                                print(f"  Processed {len(file_list)} CCDs (skipping existing)...", end='\r')
                        
                        # Start new CCD
                        current_code = line.strip()[5:].strip()  # Remove 'data_' prefix
                        current_lines = [line]
                        
                        if show_progress and len(file_list) % 100 == 0 and len(file_list) < 1000:
                            print(f"  Processed {len(file_list)} CCDs...", end='\r')
                    else:
                        # Continue current CCD
                        if current_code is not None:
                            current_lines.append(line)
                
                # Don't forget the last CCD
                if current_code is not None and current_lines:
                    file_path = get_file_path(current_code)
                    file_list.append(file_path)
                    # Only write if file doesn't exist (resume support)
                    if file_path not in existing_files:
                        files_to_create.append((file_path, current_lines))
            
            # Write files that need to be created
            if files_to_create:
                if show_progress:
                    print(f"\n  Creating {len(files_to_create)} new CCD files...")
                for file_path, lines in tqdm(files_to_create, desc="Writing files", disable=not show_progress, unit="file"):
                    output_file = os.path.join(output_dir, file_path)
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(output_file), exist_ok=True)
                    with open(output_file, 'w', encoding='utf-8') as out_f:
                        out_f.writelines(lines)
            else:
                if show_progress:
                    print(f"\n  All {len(file_list)} CCD files already exist.")
            
            if show_progress:
                print(f"\n  Split complete. Created {len(file_list)} individual CCD files.")
                print(f"  Cleaning up temporary files...")
            
            # Clean up the large files (keep individual CCDs)
            try:
                os.remove(gz_path)
                os.remove(cif_path)
            except:
                pass  # Don't fail if cleanup doesn't work
            
            return file_list
            
        except Exception as e:
            if show_progress:
                print(f"  Error: {e}")
            return []
    
    @staticmethod
    def get_http_file_list_old(base_url: str, show_progress: bool = True) -> List[str]:
        """Get list of all .cif files from HTTP/HTTPS server by parsing directory listings.
        
        Recursively scans directories to find all .cif files.
        OLD METHOD - kept for reference but not used for Set 1 anymore.
        """
        import html.parser
        from html.parser import HTMLParser
        import re
        
        files = []
        
        if show_progress:
            print("Getting directory listing from HTTP server...")
            print(f"  URL: {base_url}")
        
        class DirectoryListingParser(HTMLParser):
            """Parse HTML directory listings to find files and subdirectories."""
            def __init__(self):
                super().__init__()
                self.items = []  # Both files and directories
                
            def handle_starttag(self, tag, attrs):
                if tag == 'a':
                    for attr_name, attr_value in attrs:
                        if attr_name == 'href' and attr_value:
                            href = attr_value
                            # Skip parent directory links and root
                            if href in ['../', '..', '/', './', '']:
                                continue
                            # Remove query strings and fragments
                            if '?' in href:
                                href = href.split('?')[0]
                            if '#' in href:
                                href = href.split('#')[0]
                            # Remove leading slash if present
                            if href.startswith('/'):
                                href = href[1:]
                            # Remove trailing slash (we'll determine if it's a directory later)
                            if href.endswith('/'):
                                href = href.rstrip('/')
                            # Skip if empty after processing
                            if not href:
                                continue
                            # Add to items list (both files and directories)
                            if href not in self.items:  # Avoid duplicates
                                self.items.append(href)
        
        def get_directory_listing(url: str, is_root: bool = False) -> Tuple[List[str], List[str]]:
            """Try to get directory listing from a URL.
            
            Args:
                url: URL to get directory listing from
                is_root: True if this is the root directory (for debug output)
            
            Returns:
                Tuple of (directories, files)
            """
            directories = []
            file_list = []
            
            try:
                req = Request(url)
                req.add_header('User-Agent', 'Mozilla/5.0')
                with urlopen(req, timeout=10) as response:
                    if response.getcode() == 200:
                        content = response.read().decode('utf-8', errors='ignore')
                        
                        # Debug: print first 500 chars to see what we're getting
                        if show_progress and is_root:
                            print(f"  Debug: Response length: {len(content)} chars")
                            print(f"  Debug: First 500 chars: {content[:500]}")
                        
                        # Check if this looks like a directory listing
                        # Try multiple patterns - some servers use different HTML structures
                        if '<a href' in content.lower() or '<A HREF' in content or '<a ' in content.lower() or 'href=' in content.lower():
                            parser = DirectoryListingParser()
                            try:
                                parser.feed(content)
                            except Exception as parse_error:
                                if show_progress and is_root:
                                    print(f"  Debug: HTML parsing error: {parse_error}")
                            
                            if show_progress and is_root:
                                print(f"  Debug: Parser found {len(parser.items)} items: {parser.items[:10] if parser.items else 'none'}")
                            
                            # Determine which are directories vs files
                            for item in parser.items:
                                # Check if it's a .cif file
                                if item.endswith('.cif'):
                                    file_list.append(item)
                                # Everything else that doesn't look like a file is a directory
                                # Common file extensions to exclude
                                elif not any(item.endswith(ext) for ext in ['.html', '.txt', '.xml', '.json', '.pdf', '.zip', '.gz', '.tar', '.md', '.readme', '.cif']):
                                    # Assume it's a directory - try to scan it recursively
                                    directories.append(item)
                                # Also: items with no extension are likely directories
                                elif '.' not in item:
                                    if item not in directories:  # Avoid duplicates
                                        directories.append(item)
            except Exception as e:
                if show_progress and is_root:
                    print(f"  Debug: Error getting directory listing: {e}")
            return directories, file_list
        
        def test_directory_exists(dir_url: str) -> bool:
            """Test if a directory exists by trying to access it or a test file."""
            try:
                # Try accessing the directory URL
                req = Request(dir_url)
                req.add_header('User-Agent', 'Mozilla/5.0')
                with urlopen(req, timeout=5) as response:
                    # If we get a response (even 403/404 might mean it exists, just no listing)
                    # Try a different approach: test if we can access a common file pattern
                    return True
            except:
                return False
        
        def test_file_exists(file_url: str) -> bool:
            """Test if a file exists by trying to access it."""
            try:
                req = Request(file_url)
                req.add_header('User-Agent', 'Mozilla/5.0')
                with urlopen(req, timeout=2) as response:
                    return response.getcode() == 200
            except:
                return False
        
        def scan_directory_recursive(current_path: str, depth: int = 0, max_depth: int = 5) -> List[str]:
            """Recursively scan directories to find .cif files.
            
            Since directory listings return 403/404, we use the known file structure
            to test if directories exist by checking for files in them.
            """
            found_files = []
            
            if depth > max_depth:
                return found_files
            
            if show_progress:
                if depth == 0:
                    print(f"  Scanning directories using known structure (no directory listings available)...")
                elif depth <= 2:
                    print(f"  Scanning {current_path}...")
            
            # Try to get directory listing first (in case it works sometimes)
            dir_url = base_url.rstrip('/') + '/' + current_path if current_path else base_url.rstrip('/')
            if not dir_url.endswith('/'):
                dir_url += '/'
            directories, files_in_dir = get_directory_listing(dir_url, is_root=(depth == 0))
            
            # Add .cif files found via directory listing
            for file_name in files_in_dir:
                file_path = f"{current_path}/{file_name}" if current_path else file_name
                found_files.append(file_path)
                if show_progress and len(found_files) % 100 == 0:
                    print(f"  Found {len(found_files)} .cif files so far...", end='\r')
            
            # If directory listing worked, use it
            if directories:
                for directory in directories:
                    sub_path = f"{current_path}/{directory}" if current_path else directory
                    sub_files = scan_directory_recursive(sub_path, depth + 1, max_depth)
                    found_files.extend(sub_files)
                return found_files
            
            # Directory listing not available - use known structure patterns
            # For root level (depth 0): try directories 0-9, a-z, A-Z (last character)
            if depth == 0:
                # Check if this is EBI (flat structure) - test a few known files
                test_codes = ['001', '000', '002', 'A1A15']
                ebi_files_found = 0
                for test_code in test_codes:
                    test_url = base_url.rstrip('/') + '/' + f"{test_code}.cif"
                    if test_file_exists(test_url):
                        ebi_files_found += 1
                
                if ebi_files_found > 0:
                    # This is EBI flat structure - scan all possible codes
                    if show_progress:
                        print("  Detected EBI flat structure - scanning all possible codes...")
                    # For EBI: just {code}.cif in root
                    # Try 3-char codes: 000-999, AAA-ZZZ
                    char_set = [str(i) for i in range(10)] + [chr(i) for i in range(ord('a'), ord('z')+1)] + [chr(i) for i in range(ord('A'), ord('Z')+1)]
                    for char1 in tqdm(char_set, desc="3-char codes", disable=not show_progress, unit="code"):
                        for char2 in char_set:
                            for char3 in char_set:
                                code = char1 + char2 + char3
                                test_url = base_url.rstrip('/') + '/' + f"{code}.cif"
                                if test_file_exists(test_url):
                                    found_files.append(f"{code}.cif")
                                    if show_progress and len(found_files) % 100 == 0:
                                        print(f"  Found {len(found_files)} .cif files so far...", end='\r')
                    # Try 5-char codes
                    for char1 in tqdm(char_set, desc="5-char codes", disable=not show_progress, unit="code"):
                        for char2 in char_set:
                            for char3 in char_set:
                                for char4 in char_set:
                                    for char5 in char_set:
                                        code = char1 + char2 + char3 + char4 + char5
                                        test_url = base_url.rstrip('/') + '/' + f"{code}.cif"
                                        if test_file_exists(test_url):
                                            found_files.append(f"{code}.cif")
                                            if show_progress and len(found_files) % 100 == 0:
                                                print(f"  Found {len(found_files)} .cif files so far...", end='\r')
                    return found_files
                
                # This is wwpdb nested structure
                if show_progress:
                    print("  Detected wwpdb nested structure - scanning directories...")
                char_set = [str(i) for i in range(10)] + [chr(i) for i in range(ord('a'), ord('z')+1)] + [chr(i) for i in range(ord('A'), ord('Z')+1)]
                for last_char in tqdm(char_set, desc="Scanning top-level dirs", disable=not show_progress, unit="dir"):
                    # Test if this directory exists by trying multiple sample files
                    # Try different patterns: 000, 001, 00A, etc.
                    test_patterns = [
                        f"{last_char}/00/00{last_char}.cif",  # 000, 001, etc. (code = first_two + last_char)
                        f"{last_char}/01/01{last_char}.cif",  # 010, 011, etc.
                        f"{last_char}/10/10{last_char}.cif",  # 100, 101, etc.
                        f"{last_char}/A0/A0{last_char}.cif",  # A05 (user's example: 5/A0/A05.cif)
                        f"{last_char}/0A/0A{last_char}.cif",  # 0A0, 0A1, etc.
                    ]
                    dir_exists = False
                    for test_file in test_patterns:
                        test_url = base_url.rstrip('/') + '/' + test_file
                        if test_file_exists(test_url):
                            dir_exists = True
                            break
                    
                    if dir_exists:
                        # Directory exists, scan it
                        sub_files = scan_directory_recursive(last_char, depth + 1, max_depth)
                        found_files.extend(sub_files)
            
            # For depth 1: we're in a directory like "5" (last character)
            # Try subdirectories: first_two_chars for 3-char codes
            elif depth == 1:
                last_char = current_path
                char_set = [str(i) for i in range(10)] + [chr(i) for i in range(ord('a'), ord('z')+1)] + [chr(i) for i in range(ord('A'), ord('Z')+1)]
                for char1 in char_set:
                    for char2 in char_set:
                        first_two = char1 + char2
                        # Test if subdirectory exists by checking for a file
                        # Pattern: {last_char}/{first_two}/{code}.cif where code = first_two + last_char
                        code = first_two + last_char
                        test_file = f"{last_char}/{first_two}/{code}.cif"
                        test_url = base_url.rstrip('/') + '/' + test_file
                        if test_file_exists(test_url):
                            # Subdirectory exists, scan it for all files
                            sub_path = f"{last_char}/{first_two}"
                            sub_files = scan_directory_recursive(sub_path, depth + 1, max_depth)
                            found_files.extend(sub_files)
                # Also check for 5-char codes: {last_char}/{code}/{code}.cif
                # This requires trying many combinations, but let's do it more efficiently
                # by testing if the directory exists first
                for char1 in char_set:
                    for char2 in char_set:
                        for char3 in char_set:
                            for char4 in char_set:
                                code = char1 + char2 + char3 + char4 + last_char
                                test_file = f"{last_char}/{code}/{code}.cif"
                                test_url = base_url.rstrip('/') + '/' + test_file
                                if test_file_exists(test_url):
                                    # Found a 5-char code directory
                                    file_path = f"{last_char}/{code}/{code}.cif"
                                    found_files.append(file_path)
                                    if show_progress and len(found_files) % 100 == 0:
                                        print(f"  Found {len(found_files)} .cif files so far...", end='\r')
            
            # For depth 2: we're in a directory like "5/A05" (last_char/first_two)
            # Files should be: {last_char}/{first_two}/{code}.cif where code = first_two + last_char
            elif depth == 2:
                parts = current_path.split('/')
                if len(parts) == 2:
                    last_char, first_two = parts
                    code = first_two + last_char
                    file_path = f"{last_char}/{first_two}/{code}.cif"
                    test_url = base_url.rstrip('/') + '/' + file_path
                    if test_file_exists(test_url):
                        found_files.append(file_path)
                        if show_progress and len(found_files) % 100 == 0:
                            print(f"  Found {len(found_files)} .cif files so far...", end='\r')
            
            return found_files
        
        # Start recursive scanning
        files = scan_directory_recursive('', max_depth=5)
        
        if show_progress:
            print(f"\nScanning complete. Found {len(files)} .cif files.")
        return files
    
    @staticmethod
    def download_http_file(base_url: str, file_path: str, local_path: str, skip_existing: bool = True):
        """Download a file from HTTP/HTTPS, preserving directory structure.
        
        Args:
            base_url: Base URL for the file
            file_path: Relative path to the file
            local_path: Local path where file should be saved
            skip_existing: If True, skip download if file already exists (resume support)
        """
        # Skip if file already exists (resume support)
        if skip_existing and os.path.exists(local_path):
            return
        
        # Construct full URL
        full_url = base_url.rstrip('/') + '/' + file_path
        
        try:
            # Create local directory structure
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            req = Request(full_url)
            req.add_header('User-Agent', 'Mozilla/5.0')
            with urlopen(req, timeout=30) as response:
                if response.getcode() == 200:
                    with open(local_path, 'wb') as f:
                        f.write(response.read())
                else:
                    print(f"Error downloading {file_path}: HTTP {response.getcode()}")
        except Exception as e:
            print(f"Error downloading {file_path}: {e}")
    
    @staticmethod
    def get_github_file_list(repo_url: str, show_progress: bool = True, github_token: Optional[str] = None) -> List[str]:
        """Get list of all .cif files from GitHub using API.
        
        Args:
            repo_url: GitHub repository URL
            show_progress: Whether to show progress messages
            github_token: Optional GitHub personal access token for higher rate limits
        """
        # Convert GitHub web URL to API URL
        # https://github.com/MonomerLibrary/monomers/tree/master/
        # -> https://api.github.com/repos/MonomerLibrary/monomers/contents/
        
        api_url = repo_url.replace('https://github.com/', 'https://api.github.com/repos/')
        api_url = api_url.replace('/tree/master/', '/contents/')
        if not api_url.endswith('/'):
            api_url += '/'
        
        files = []
        dirs_processed = 0
        
        def get_files_recursive(url: str):
            """Recursively get files from GitHub API."""
            nonlocal dirs_processed
            try:
                req = Request(url)
                req.add_header('Accept', 'application/vnd.github.v3+json')
                req.add_header('User-Agent', 'Mozilla/5.0')
                if github_token:
                    req.add_header('Authorization', f'token {github_token}')
                with urlopen(req, timeout=30) as response:
                    if response.getcode() == 403:
                        error_msg = response.read().decode('utf-8')
                        if show_progress:
                            print(f"\nError accessing {url}: HTTP Error 403: rate limit exceeded")
                            if not github_token:
                                print("Tip: Use --github-token to increase rate limits. Get a token at https://github.com/settings/tokens")
                        return
                    items = json.loads(response.read().decode('utf-8'))
                
                if not isinstance(items, list):
                    return
                
                for item in items:
                    if item.get('type') == 'file' and item.get('name', '').endswith('.cif'):
                        files.append(item['path'])
                        if show_progress and len(files) % 100 == 0:
                            print(f"  Found {len(files)} .cif files so far...", end='\r')
                    elif item.get('type') == 'dir':
                        dirs_processed += 1
                        if show_progress and dirs_processed % 10 == 0:
                            print(f"  Scanning directories... Found {len(files)} .cif files so far...", end='\r')
                        get_files_recursive(item['url'])
            except Exception as e:
                error_str = str(e).lower()
                if '403' in error_str or 'rate limit' in error_str:
                    if show_progress:
                        print(f"\nError accessing {url}: HTTP Error 403: rate limit exceeded")
                        if not github_token:
                            print("Tip: Use --github-token to increase rate limits. Get a token at https://github.com/settings/tokens")
                elif show_progress:
                    print(f"\nError accessing {url}: {e}")
        
        if show_progress:
            print("Connecting to GitHub API...")
        get_files_recursive(api_url)
        if show_progress:
            print(f"\nScanning complete. Found {len(files)} .cif files.")
        return files
    
    @staticmethod
    def download_github_file(repo_url: str, file_path: str, local_path: str, skip_existing: bool = True):
        """Download a file from GitHub, preserving directory structure.
        
        Args:
            repo_url: GitHub repository URL
            file_path: Relative path to the file in the repo
            local_path: Local path where file should be saved
            skip_existing: If True, skip download if file already exists (resume support)
        """
        # Skip if file already exists (resume support)
        if skip_existing and os.path.exists(local_path):
            return
        
        # Convert to raw content URL
        raw_url = repo_url.replace('https://github.com/', 'https://raw.githubusercontent.com/')
        raw_url = raw_url.replace('/tree/master/', '/master/')
        if not raw_url.endswith('/'):
            raw_url += '/'
        raw_url += file_path
        
        try:
            with urlopen(raw_url, timeout=30) as response:
                if response.getcode() == 200:
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    with open(local_path, 'wb') as f:
                        f.write(response.read())
                else:
                    print(f"Error downloading {raw_url}: HTTP {response.getcode()}")
        except Exception as e:
            print(f"Error downloading {file_path}: {e}")
    
    @staticmethod
    def get_http_file_content(base_url: str, file_path: str) -> Optional[str]:
        """Get file content from HTTP/HTTPS without saving to disk."""
        try:
            file_url = base_url.rstrip('/') + '/' + file_path
            req = Request(file_url)
            req.add_header('User-Agent', 'Mozilla/5.0')
            with urlopen(req, timeout=30) as response:
                if response.getcode() == 200:
                    return response.read().decode('utf-8', errors='ignore')
                else:
                    return None
        except Exception as e:
            return None
    
    @staticmethod
    def get_github_file_content(repo_url: str, file_path: str) -> Optional[str]:
        """Get file content from GitHub without saving to disk."""
        # Convert to raw content URL
        raw_url = repo_url.replace('https://github.com/', 'https://raw.githubusercontent.com/')
        raw_url = raw_url.replace('/tree/master/', '/master/')
        if not raw_url.endswith('/'):
            raw_url += '/'
        raw_url += file_path
        
        try:
            req = Request(raw_url)
            req.add_header('User-Agent', 'Mozilla/5.0')
            with urlopen(req, timeout=30) as response:
                if response.getcode() == 200:
                    return response.read().decode('utf-8', errors='ignore')
                else:
                    return None
        except Exception as e:
            return None


class ComparisonEngine:
    """Engine for comparing mmCIF files."""
    
    def __init__(self, correlation_table_path: str):
        self.correlations = self._load_correlation_table(correlation_table_path)
    
    def _load_correlation_table(self, csv_path: str) -> List[Tuple[List[str], List[str], bool]]:
        """Load correlation table from CSV.
        Returns list of tuples: (set1_items, set2_items, same_name)
        """
        correlations = []
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                set1_item = row['wwpdbccd'].strip()
                set2_item = row['ccp4monomerlibrary'].strip()
                same_name = row.get('same_name', 'N').strip().upper() == 'Y'
                
                if set1_item and set2_item:
                    correlations.append(([set1_item], [set2_item], same_name))
        
        return correlations
    
    def _group_correlations_by_category(self, correlations: List[Tuple[List[str], List[str], bool]]) -> Dict[str, List[Tuple[List[str], List[str], bool]]]:
        """Group correlations by category for grouped comparisons."""
        grouped = defaultdict(list)
        
        for set1_items, set2_items, same_name in correlations:
            # Get category from first item
            category = set1_items[0].split('.')[0]
            grouped[category].append((set1_items, set2_items, same_name))
        
        return grouped
    
    def _normalize_value(self, value: str) -> str:
        """Normalize a value for comparison."""
        if value is None:
            return ''
        
        value = str(value).strip()
        
        # Remove quotes
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        elif value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        
        # Remove newlines (they're formatting artifacts, not actual content)
        # This handles multi-line values where newlines split words
        value = value.replace('\r\n', '').replace('\n', '').replace('\r', '')
        
        # Convert to lowercase
        value = value.lower()
        
        return value
    
    def _normalize_bond_order(self, value: str) -> str:
        """Normalize bond order values (SING/DOUB vs SINGLE/DOUBLE)."""
        value = self._normalize_value(value)
        if value == 'sing':
            return 'single'
        elif value == 'doub':
            return 'double'
        return value
    
    def _get_item_value(self, parser: mmCIFParser, item_path: str) -> Optional[str]:
        """Get value for an item path like '_chem_comp.name' or from loop data."""
        # Check single values first (with underscore)
        value = parser.get_value(item_path)
        if value is not None and value != '':
            return value
        
        # Try without leading underscore (for multi-line values stored without underscore)
        item_path_no_underscore = item_path.lstrip('_')
        if item_path_no_underscore != item_path:
            value = parser.get_value(item_path_no_underscore)
            if value is not None and value != '':
                return value
        
        # Check loop data - try with and without underscore prefix
        category = item_path.split('.')[0]
        category_key = category.lstrip('_')
        
        loop_data = parser.get_loop_data(category_key)
        if loop_data:
            # Return first matching value from first row
            if item_path in loop_data[0]:
                return loop_data[0][item_path]
            # Also try without leading underscore
            if item_path_no_underscore in loop_data[0]:
                return loop_data[0][item_path_no_underscore]
        
        return None
    
    def _get_grouped_values(self, parser: mmCIFParser, item_paths: List[str]) -> List[Tuple]:
        """Get grouped values from loop data (e.g., all atom records)."""
        if not item_paths:
            return []
        
        category = item_paths[0].split('.')[0]
        # Remove leading underscore if present for lookup
        category_key = category.lstrip('_')
        loop_data = parser.get_loop_data(category_key)
        
        if not loop_data:
            return []
        
        results = []
        for row in loop_data:
            values = []
            for item_path in item_paths:
                value = row.get(item_path, '')
                values.append(value)
            results.append(tuple(values))
        
        return results
    
    def _get_grouped_values_from_category(self, parser: mmCIFParser, category: str, item_paths: List[str]) -> List[Tuple]:
        """Get grouped values from a specific category (for cases where category name differs)."""
        if not item_paths:
            return []
        
        # Remove leading underscore if present for lookup
        category_key = category.lstrip('_')
        loop_data = parser.get_loop_data(category_key)
        
        if not loop_data:
            return []
        
        results = []
        for row in loop_data:
            values = []
            for item_path in item_paths:
                value = row.get(item_path, '')
                values.append(value)
            results.append(tuple(values))
        
        return results
    
    def compare_items(self, parser1: mmCIFParser, parser2: mmCIFParser, 
                     set1_items: List[str], set2_items: List[str]) -> bool:
        """Compare a group of items between two parsers."""
        # Get values from both sets
        if len(set1_items) == 1 and len(set2_items) == 1:
            # Single item comparison
            val1 = self._get_item_value(parser1, set1_items[0])
            val2 = self._get_item_value(parser2, set2_items[0])
            
            # Special handling for bond order
            if 'value_order' in set1_items[0] or 'type' in set2_items[0]:
                val1 = self._normalize_bond_order(val1) if val1 else ''
                val2 = self._normalize_bond_order(val2) if val2 else ''
            else:
                val1 = self._normalize_value(val1) if val1 else ''
                val2 = self._normalize_value(val2) if val2 else ''
            
            return val1 == val2
        else:
            # Grouped comparison (e.g., atoms, bonds)
            # Handle special case for descriptors where categories and field names differ
            if 'description_generator' in '/'.join(set2_items):
                # Set2 uses _pdbx_chem_comp_description_generator category
                # Map: set1 (type/program/program_version/descriptor) -> set2 (comp_id/program_name/program_version/descriptor)
                # We'll compare program/program_version/descriptor (ignoring type/comp_id)
                category1 = set1_items[0].split('.')[0]
                category2 = 'pdbx_chem_comp_description_generator'
                
                # Get all fields from set1
                group1_full = self._get_grouped_values_from_category(parser1, category1, set1_items)
                # Get all fields from set2
                group2_full = self._get_grouped_values_from_category(parser2, category2, set2_items)
                
                # Extract relevant fields for comparison (skip type/comp_id, compare program/program_version/descriptor)
                # set1: [type, program, program_version, descriptor] -> use [1, 2, 3]
                # set2: [comp_id, program_name, program_version, descriptor] -> use [1, 2, 3]
                group1 = [tuple(row[1:]) for row in group1_full]  # Skip first element (type)
                group2 = [tuple(row[1:]) for row in group2_full]  # Skip first element (comp_id)
            else:
                group1 = self._get_grouped_values(parser1, set1_items)
                group2 = self._get_grouped_values(parser2, set2_items)
            
            if len(group1) != len(group2):
                return False
            
            # Normalize and sort for comparison
            normalized1 = []
            normalized2 = []
            
            # Check if this is a bond comparison (has atom_id_1 and atom_id_2)
            is_bond_comparison = ('atom_id_1' in '/'.join(set1_items) and 
                                 'atom_id_2' in '/'.join(set1_items))
            
            for values in group1:
                normalized = []
                for i, val in enumerate(values):
                    item_path = set1_items[i]
                    if 'value_order' in item_path or 'type' in item_path:
                        normalized.append(self._normalize_bond_order(val))
                    else:
                        normalized.append(self._normalize_value(val))
                
                # For bonds, normalize atom pair order (bonds are undirected)
                if is_bond_comparison and len(normalized) >= 2:
                    # Swap atom1 and atom2 if needed to ensure consistent ordering
                    if normalized[0] > normalized[1]:
                        normalized[0], normalized[1] = normalized[1], normalized[0]
                
                normalized1.append(tuple(normalized))
            
            for values in group2:
                normalized = []
                for i, val in enumerate(values):
                    item_path = set2_items[i]
                    if 'value_order' in item_path or 'type' in item_path:
                        normalized.append(self._normalize_bond_order(val))
                    else:
                        normalized.append(self._normalize_value(val))
                
                # For bonds, normalize atom pair order (bonds are undirected)
                if is_bond_comparison and len(normalized) >= 2:
                    # Swap atom1 and atom2 if needed to ensure consistent ordering
                    if normalized[0] > normalized[1]:
                        normalized[0], normalized[1] = normalized[1], normalized[0]
                
                normalized2.append(tuple(normalized))
            
            # Sort both lists for comparison
            normalized1.sort()
            normalized2.sort()
            
            return normalized1 == normalized2
    
    def compare_all(self, parser1: mmCIFParser, parser2: mmCIFParser) -> Dict[str, bool]:
        """Compare all items according to correlation table.
        Returns a dictionary with keys for each comparison group.
        """
        results = {}
        
        # Group correlations by category
        grouped_correlations = self._group_correlations_by_category(self.correlations)
        
        # Define comparison groups based on original requirements
        # These groups determine what gets compared together
        comparison_groups = {
            'name': ['_chem_comp.name'],
            'type': ['_chem_comp.type'],
            'atom': ['_chem_comp_atom.atom_id', '_chem_comp_atom.type_symbol', '_chem_comp_atom.charge'],
            'bond': ['_chem_comp_bond.atom_id_1', '_chem_comp_bond.atom_id_2', 
                     '_chem_comp_bond.value_order', '_chem_comp_bond.pdbx_aromatic_flag'],
            'descriptor': ['_pdbx_chem_comp_descriptor.type', '_pdbx_chem_comp_descriptor.program',
                          '_pdbx_chem_comp_descriptor.program_version', '_pdbx_chem_comp_descriptor.descriptor']
        }
        
        # Compare name
        name_corr = [c for c in self.correlations if c[0][0] == '_chem_comp.name']
        if name_corr:
            set1_items, set2_items, _ = name_corr[0]
            results['name'] = self.compare_items(parser1, parser2, set1_items, set2_items)
        
        # Compare type
        type_corr = [c for c in self.correlations if c[0][0] == '_chem_comp.type']
        if type_corr:
            set1_items, set2_items, _ = type_corr[0]
            results['type'] = self.compare_items(parser1, parser2, set1_items, set2_items)
        
        # Compare atoms (grouped)
        atom_corrs = [c for c in self.correlations 
                     if c[0][0].startswith('_chem_comp_atom.') 
                     and c[0][0] in ['_chem_comp_atom.atom_id', '_chem_comp_atom.type_symbol', '_chem_comp_atom.charge']]
        if atom_corrs:
            # Create a mapping for easy lookup
            corr_map = {c[0][0]: c[1][0] for c in atom_corrs}
            # Reorder to match: atom_id, type_symbol, charge
            order = ['_chem_comp_atom.atom_id', '_chem_comp_atom.type_symbol', '_chem_comp_atom.charge']
            set1_items = [item for item in order if item in corr_map]
            set2_items = [corr_map[item] for item in set1_items]
            results['atom'] = self.compare_items(parser1, parser2, set1_items, set2_items)
        
        # Compare bonds (grouped)
        bond_corrs = [c for c in self.correlations 
                     if c[0][0].startswith('_chem_comp_bond.') 
                     and c[0][0] in ['_chem_comp_bond.atom_id_1', '_chem_comp_bond.atom_id_2', 
                                     '_chem_comp_bond.value_order', '_chem_comp_bond.pdbx_aromatic_flag']]
        if bond_corrs:
            # Create a mapping for easy lookup
            corr_map = {c[0][0]: c[1][0] for c in bond_corrs}
            # Reorder to match: atom_id_1, atom_id_2, value_order, pdbx_aromatic_flag
            order = ['_chem_comp_bond.atom_id_1', '_chem_comp_bond.atom_id_2', 
                    '_chem_comp_bond.value_order', '_chem_comp_bond.pdbx_aromatic_flag']
            set1_items = [item for item in order if item in corr_map]
            set2_items = [corr_map[item] for item in set1_items]
            results['bond'] = self.compare_items(parser1, parser2, set1_items, set2_items)
        
        # Compare descriptors (grouped) - use _pdbx_chem_comp_descriptor from set2 (lines 18-22)
        # Filter to only include mappings where set2 also uses _pdbx_chem_comp_descriptor (not description_generator)
        desc_corrs = [c for c in self.correlations 
                     if c[0][0].startswith('_pdbx_chem_comp_descriptor.') 
                     and c[1][0].startswith('_pdbx_chem_comp_descriptor.')
                     and c[0][0] in ['_pdbx_chem_comp_descriptor.type', '_pdbx_chem_comp_descriptor.program',
                                     '_pdbx_chem_comp_descriptor.program_version', '_pdbx_chem_comp_descriptor.descriptor']]
        if desc_corrs:
            # Create a mapping for easy lookup
            corr_map = {c[0][0]: c[1][0] for c in desc_corrs}
            # Reorder to match: type, program, program_version, descriptor
            order = ['_pdbx_chem_comp_descriptor.type', '_pdbx_chem_comp_descriptor.program',
                    '_pdbx_chem_comp_descriptor.program_version', '_pdbx_chem_comp_descriptor.descriptor']
            set1_items = [item for item in order if item in corr_map]
            set2_items = [corr_map[item] for item in set1_items]
            results['descriptor'] = self.compare_items(parser1, parser2, set1_items, set2_items)
        
        return results


def compare_file_pair_worker(args_tuple):
    """Worker function to compare a single file pair (module-level for multiprocessing)."""
    file_pair, mode, correlation_table_path, github_token_val, set2_dates_cache, batch_fetching_attempted = args_tuple
    file1, file2 = file_pair
    
    try:
        # Initialize comparison engine (each worker needs its own instance)
        comparison_engine = ComparisonEngine(correlation_table_path)
        
        # Handle online mode (file1 and file2 are tuples)
        if mode == 'online':
            source1_type, base1, path1 = file1
            source2_type, base2, path2 = file2
            
            # Fetch content from remote sources
            if source1_type == 'http':
                content1 = FileDownloader.get_http_file_content(base1, path1)
            else:
                content1 = FileDownloader.get_github_file_content(base1, path1)
            
            if source2_type == 'http':
                content2 = FileDownloader.get_http_file_content(base2, path2)
            else:
                content2 = FileDownloader.get_github_file_content(base2, path2)
            
            if content1 is None or content2 is None:
                return None
            
            parser1 = mmCIFParser(content=content1)
            parser2 = mmCIFParser(content=content2)
            file_name = get_file_name_from_path(path1)
        else:
            # Local/download mode - use file paths
            parser1 = mmCIFParser(file1)
            parser2 = mmCIFParser(file2)
            file_name = get_file_name_from_path(file1)
            path2 = file2  # For date retrieval
        
        comparison_results = comparison_engine.compare_all(parser1, parser2)
        
        # Extract individual comparison results
        name_match = comparison_results.get('name', False)
        type_match = comparison_results.get('type', False)
        atom_match = comparison_results.get('atom', False)
        bond_match = comparison_results.get('bond', False)
        descriptor_match = comparison_results.get('descriptor', False)
        
        overall_match = 'Y' if all([name_match, type_match, atom_match, bond_match, descriptor_match]) else 'N'
        
        if mode == 'online':
            set1_date = get_modified_date(parser1, None, use_file_date=False)
            file2_name = os.path.basename(path2)
            # Try cache first - if batch fetching was attempted, don't make individual API calls
            set2_date = None
            if batch_fetching_attempted:
                # Batch fetching was attempted, only use cache
                set2_date = set2_dates_cache.get(file2_name)
                # Don't make individual API calls if cache lookup fails - batch fetching already tried
            else:
                # No batch fetching attempted, make individual API call
                set2_date = get_modified_date(parser2, None, use_file_date=True, 
                                             file_name=file2_name, 
                                             repo_url="https://github.com/MonomerLibrary/monomers",
                                             github_token=github_token_val,
                                             use_file_fallback=False)
        else:
            set1_date = get_modified_date(parser1, file1, use_file_date=False)
            file2_name = os.path.basename(file2)
            # Try cache first - if batch fetching was attempted, don't make individual API calls
            set2_date = None
            if batch_fetching_attempted:
                # Batch fetching was attempted, only use cache
                set2_date = set2_dates_cache.get(file2_name)
                # Don't make individual API calls if cache lookup fails - batch fetching already tried
            else:
                # No batch fetching attempted, make individual API call
                set2_date = get_modified_date(parser2, file2, use_file_date=True, 
                                             file_name=file2_name, 
                                             repo_url="https://github.com/MonomerLibrary/monomers",
                                             github_token=github_token_val,
                                             use_file_fallback=False)
        
        return {
            'ccd_code': file_name,
            'name_identical': 'Y' if name_match else 'N',
            'type_identical': 'Y' if type_match else 'N',
            'atom_identical': 'Y' if atom_match else 'N',
            'bond_identical': 'Y' if bond_match else 'N',
            'descriptor_identical': 'Y' if descriptor_match else 'N',
            'overall_identical': overall_match,
            'wwpdb_modified_date': set1_date or '',
            'ccp4_modified_date': set2_date or ''
        }
    except Exception as e:
        # Return error result instead of crashing
        file_name = get_file_name_from_path(file1 if not isinstance(file1, tuple) else file1[2])
        return {
            'ccd_code': file_name,
            'name_identical': 'ERROR',
            'type_identical': 'ERROR',
            'atom_identical': 'ERROR',
            'bond_identical': 'ERROR',
            'descriptor_identical': 'ERROR',
            'overall_identical': 'ERROR',
            'wwpdb_modified_date': '',
            'ccp4_modified_date': ''
        }

def get_file_name_from_path(file_path: str) -> str:
    """Extract file name without extension from path."""
    return os.path.splitext(os.path.basename(file_path))[0]


# Cache for GitHub commit dates to avoid repeated API calls
_github_commit_cache = {}
# Batch queue for collecting file names before making batch API calls
_github_batch_queue = []
_github_batch_size = 50  # Process in batches of 50 files

def get_github_commit_dates_batch(file_names: List[str], repo_url: str = "https://github.com/MonomerLibrary/monomers",
                                 github_token: Optional[str] = None) -> Dict[str, Optional[str]]:
    """Get commit dates for multiple files using GitHub GraphQL API (batch query).
    
    Args:
        file_names: List of file names (e.g., ['000.cif', '001.cif'])
        repo_url: GitHub repository URL
        github_token: Optional GitHub personal access token
    
    Returns:
        Dictionary mapping file_name -> date_string or None
    """
    results = {}
    
    # Extract owner and repo from URL
    # https://github.com/MonomerLibrary/monomers -> MonomerLibrary/monomers
    repo_path = repo_url.replace('https://github.com/', '').rstrip('/')
    owner, repo = repo_path.split('/')[:2]
    
    # Build GraphQL query for multiple files
    # We'll query in chunks to avoid query size limits
    chunk_size = 50
    for i in range(0, len(file_names), chunk_size):
        chunk = file_names[i:i+chunk_size]
        
        # Build GraphQL query using commits API (history doesn't exist on Blob)
        queries = []
        for idx, file_name in enumerate(chunk):
            # Determine file path (try structured path first)
            # Repository uses lowercase directory names (e.g., o/ONS.cif, not O/ONS.cif)
            if len(file_name) >= 1:
                file_path = f"{file_name[0].lower()}/{file_name}"
            else:
                file_path = file_name
            
            # GraphQL query for this file - query commits filtered by path
            query_alias = f"file{idx}"
            queries.append(f'''
                {query_alias}: repository(owner: "{owner}", name: "{repo}") {{
                    defaultBranchRef {{
                        target {{
                            ... on Commit {{
                                history(first: 1, path: "{file_path}") {{
                                    nodes {{
                                        committedDate
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            ''')
        
        graphql_query = f'''
        query {{
            {''.join(queries)}
        }}
        '''
        
        try:
            import json
            req = Request('https://api.github.com/graphql')
            req.add_header('Content-Type', 'application/json')
            req.add_header('User-Agent', 'Mozilla/5.0')
            if github_token:
                req.add_header('Authorization', f'Bearer {github_token}')
            
            data = json.dumps({'query': graphql_query}).encode('utf-8')
            req.add_header('Content-Length', str(len(data)))
            
            with urlopen(req, data=data, timeout=30) as response:
                if response.getcode() == 200:
                    result = json.loads(response.read().decode('utf-8'))
                    if 'data' in result:
                        # Parse results - new structure: repository.defaultBranchRef.target.history
                        for idx, file_name in enumerate(chunk):
                            query_alias = f"file{idx}"
                            file_data = result['data'].get(query_alias, {})
                            
                            # Check if there's an error for this specific file
                            if file_data is None:
                                results[file_name] = None
                                continue
                            
                            # Navigate: repository -> defaultBranchRef -> target -> history
                            default_branch_ref = file_data.get('defaultBranchRef', {})
                            target = default_branch_ref.get('target', {})
                            history = target.get('history', {}).get('nodes', [])
                            
                            if history and len(history) > 0:
                                commit_date = history[0].get('committedDate', '')
                                if commit_date:
                                    import datetime
                                    dt = datetime.datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                                    date_str = dt.strftime('%Y-%m-%d')
                                    results[file_name] = date_str
                                    # Cache the result
                                    cache_key = f"{repo_url}:{file_name}"
                                    _github_commit_cache[cache_key] = date_str
                                else:
                                    results[file_name] = None
                            else:
                                # No history found - file might not exist or path is wrong
                                # Try root path as fallback (but don't retry in same batch to avoid complexity)
                                results[file_name] = None
                                cache_key = f"{repo_url}:{file_name}"
                                _github_commit_cache[cache_key] = None
                    elif 'errors' in result:
                        # Handle errors - might be rate limit or file not found
                        error_messages = []
                        for error in result.get('errors', []):
                            error_msg = error.get('message', 'Unknown error')
                            error_messages.append(error_msg)
                            if 'rate limit' in error_msg.lower():
                                if not hasattr(get_github_commit_dates_batch, '_rate_limit_warned'):
                                    print("\nWarning: GitHub API rate limit exceeded. Set 2 commit dates will be missing.")
                                    print("Tip: Use --github-token to increase rate limits.")
                                    get_github_commit_dates_batch._rate_limit_warned = True
                            elif 'Could not resolve' not in error_msg:  # File not found is expected for some files
                                # Print other errors for debugging (but only once)
                                if not hasattr(get_github_commit_dates_batch, '_other_errors_shown'):
                                    print(f"\nWarning: GitHub GraphQL API errors: {', '.join(error_messages[:3])}")
                                    if len(error_messages) > 3:
                                        print(f"  ... and {len(error_messages) - 3} more errors")
                                    get_github_commit_dates_batch._other_errors_shown = True
                        # Mark all as None on error
                        for file_name in chunk:
                            results[file_name] = None
                    else:
                        # Unexpected response format
                        if not hasattr(get_github_commit_dates_batch, '_unexpected_response_shown'):
                            print(f"\nWarning: Unexpected response from GitHub API (no 'data' or 'errors' key)")
                            get_github_commit_dates_batch._unexpected_response_shown = True
                        for file_name in chunk:
                            results[file_name] = None
                elif response.getcode() == 403:
                    if not hasattr(get_github_commit_dates_batch, '_rate_limit_warned'):
                        print("\nWarning: GitHub API rate limit exceeded. Set 2 commit dates will be missing.")
                        get_github_commit_dates_batch._rate_limit_warned = True
                    for file_name in chunk:
                        results[file_name] = None
        except Exception as e:
            # On error, mark all as None
            for file_name in chunk:
                results[file_name] = None
    
    return results

def get_github_commit_date(file_name: str, repo_url: str = "https://github.com/MonomerLibrary/monomers", 
                          github_token: Optional[str] = None) -> Optional[str]:
    """Get the last commit date for a file from GitHub.
    
    Uses caching to avoid repeated API calls for the same file.
    
    Args:
        file_name: Name of the file (e.g., '000.cif')
        repo_url: GitHub repository URL
        github_token: Optional GitHub personal access token for higher rate limits
    
    Returns:
        Date string in YYYY-MM-DD format, or None if not found
    """
    # Check cache first
    cache_key = f"{repo_url}:{file_name}"
    if cache_key in _github_commit_cache:
        return _github_commit_cache[cache_key]
    
    try:
        # Convert repository URL to API URL
        # https://github.com/MonomerLibrary/monomers -> https://api.github.com/repos/MonomerLibrary/monomers
        api_base = repo_url.replace('https://github.com/', 'https://api.github.com/repos/')
        
        # Determine file path in repository
        # Files are organized as: first_char/filename.cif (e.g., 0/000.cif or o/ONS.cif)
        # Note: Set2 (GitHub) uses lowercase directory names (o/ONS.cif), 
        #       while Set1 uses uppercase (O/ONS.cif)
        # Based on: https://github.com/MonomerLibrary/monomers/blob/master/o/ONS.cif
        file_paths_to_try = []
        
        if len(file_name) >= 1:
            # Try structured path: o/ONS.cif (first character lowercase, then full filename)
            # Set2 (GitHub) uses lowercase directory names
            file_paths_to_try.append(f"{file_name[0].lower()}/{file_name}")
        # Also try root: ONS.cif (fallback)
        file_paths_to_try.append(file_name)
        
        for file_path in file_paths_to_try:
            # Get commits for this file path
            commits_url = f"{api_base}/commits?path={file_path}&per_page=1"
            
            try:
                # Removed sleep delay - caching handles rate limiting better
                req = Request(commits_url)
                req.add_header('Accept', 'application/vnd.github.v3+json')
                req.add_header('User-Agent', 'Mozilla/5.0')
                if github_token:
                    req.add_header('Authorization', f'token {github_token}')
                with urlopen(req, timeout=30) as response:
                    if response.getcode() == 200:
                        commits = json.loads(response.read().decode('utf-8'))
                        if commits and len(commits) > 0:
                            # Get commit date
                            commit_date = commits[0].get('commit', {}).get('committer', {}).get('date', '')
                            if commit_date:
                                # Parse ISO 8601 date and convert to YYYY-MM-DD
                                import datetime
                                dt = datetime.datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                                date_str = dt.strftime('%Y-%m-%d')
                                # Cache the result
                                _github_commit_cache[cache_key] = date_str
                                return date_str
                    elif response.getcode() == 403:
                        # Rate limit exceeded
                        # Print warning only once per run
                        if not hasattr(get_github_commit_date, '_rate_limit_warned'):
                            print("\nWarning: GitHub API rate limit exceeded. Set 2 commit dates will be missing.")
                            print("Tip: Use --github-token to increase rate limits. Get a token at https://github.com/settings/tokens")
                            get_github_commit_date._rate_limit_warned = True
                        # Don't cache rate limit failures - might succeed later
                        return None
            except Exception as e:
                error_str = str(e).lower()
                # If rate limited, stop trying
                if 'rate limit' in error_str or '403' in error_str or 'forbidden' in error_str:
                    if not hasattr(get_github_commit_date, '_rate_limit_warned'):
                        print("\nWarning: GitHub API rate limit exceeded. Set 2 commit dates will be missing.")
                        print("Tip: Use --github-token to increase rate limits. Get a token at https://github.com/settings/tokens")
                        get_github_commit_date._rate_limit_warned = True
                    return None
                # Try next path
                continue
        
        # Cache None result to avoid retrying failed lookups
        _github_commit_cache[cache_key] = None
        return None
    except Exception as e:
        # Silently fail - return None
        return None


def get_modified_date(parser: mmCIFParser, file_path: Optional[str] = None, use_file_date: bool = False, 
                     file_name: Optional[str] = None, repo_url: Optional[str] = None, 
                     github_token: Optional[str] = None, use_file_fallback: bool = True) -> Optional[str]:
    """Get modified date from parser, file system, or GitHub.
    
    Args:
        parser: The mmCIF parser
        file_path: Path to the file (for getting filesystem modification date)
        use_file_date: If True, use GitHub commit date for set2. If False, use parser only.
        file_name: Name of the file (e.g., '000.cif') for GitHub API lookup
        repo_url: GitHub repository URL for set2 files
    
    For set1: Use parser (_chem_comp.pdbx_modified_date) only
    For set2: Use last commit date from GitHub
    """
    if use_file_date:
        # For set2, use GitHub commit date
        if file_name:
            commit_date = get_github_commit_date(file_name, repo_url, github_token)
            if commit_date:
                return commit_date
        
        # Fallback to file modification date if GitHub API fails (only if use_file_fallback is True)
        if use_file_fallback and file_path and os.path.exists(file_path):
            try:
                import datetime
                mod_time = os.path.getmtime(file_path)
                mod_date = datetime.datetime.fromtimestamp(mod_time)
                return mod_date.strftime('%Y-%m-%d')
            except Exception:
                pass
        return None
    else:
        # For set1, use parser only
        return parser.get_value('_chem_comp.pdbx_modified_date')


def main():
    parser = argparse.ArgumentParser(description='Compare mmCIF files from two sources')
    parser.add_argument('--mode', choices=['local', 'download', 'online', 'refetch-dates'], 
                       default='local',
                       help='Mode: local (use local files), download (download all files), online (compare directly from remote sources), or refetch-dates (re-fetch Set 2 dates from a previous output CSV)')
    parser.add_argument('--download-set1', action='store_true',
                       help='Download Set 1 files from HTTP archive (use with --mode download)')
    parser.add_argument('--download-set2', action='store_true',
                       help='Download Set 2 files from GitHub (use with --mode download)')
    parser.add_argument('--set1-dir', default='set1_files',
                       help='Directory for set1 files (default: set1_files)')
    parser.add_argument('--set2-dir', default='set2_files',
                       help='Directory for set2 files (default: set2_files)')
    parser.add_argument('--output', default='comparison_results.csv',
                       help='Output CSV file (default: comparison_results.csv)')
    parser.add_argument('--correlation-table', default=None,
                       help='Path to correlation table CSV (required for comparison, not needed for --download-only)')
    parser.add_argument('--ccd-codes', type=str, default=None,
                       help='Comma-separated list of CCD codes to compare (e.g., "000,001,A1A15"). If not specified, compares all files. Works with --mode online.')
    parser.add_argument('--github-token', type=str, default=None,
                       help='GitHub personal access token for higher API rate limits (optional). Get one at https://github.com/settings/tokens')
    parser.add_argument('--download-only', action='store_true',
                       help='Only download files without comparing (use with --mode download)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit the number of file pairs to compare (useful for testing, e.g., --limit 1000)')
    parser.add_argument('--input-csv', type=str, default=None,
                       help='Input CSV file to read (required for --mode refetch-dates)')
    
    args = parser.parse_args()
    
    # Read GitHub token from file if not provided via command line
    # Needed for modes that access GitHub (download, online) and for getting Set 2 dates in local mode
    github_token = args.github_token
    if not github_token:
        token_file = Path('github_token.txt')
        if token_file.exists():
            try:
                github_token = token_file.read_text().strip()
                if github_token:
                    print("Using GitHub token from github_token.txt")
            except Exception as e:
                print(f"Warning: Could not read github_token.txt: {e}")
    
    # Handle refetch-dates mode
    if args.mode == 'refetch-dates':
        if not args.input_csv:
            print("Error: --input-csv is required for --mode refetch-dates.")
            print("Example: python ccd_sync.py --mode refetch-dates --input-csv comparison_results_20260107_213618.csv")
            sys.exit(1)
        
        if not os.path.exists(args.input_csv):
            print(f"Error: Input CSV file not found: {args.input_csv}")
            sys.exit(1)
        
        print(f"Reading input CSV: {args.input_csv}")
        
        # Read the CSV file
        rows = []
        with open(args.input_csv, 'r', newline='') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
        
        print(f"Found {len(rows)} rows in input CSV")
        
        # Get today's date for comparison
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        # Filter rows that need date refetching (missing or today's date)
        rows_to_refetch = []
        ccd_to_filename = {}
        ccd_to_row = {}  # Map CCD code to row for easy updates
        
        for row in rows:
            ccd_code = row.get('ccd_code', '').strip()
            if ccd_code:
                current_date = row.get('ccp4_modified_date', '').strip()
                # Only refetch if date is missing or equals today's date (likely a placeholder)
                if not current_date or current_date == today_str:
                    filename = f"{ccd_code}.cif"
                    ccd_to_filename[ccd_code] = filename
                    ccd_to_row[ccd_code] = row
                    rows_to_refetch.append(ccd_code)
        
        total_rows = len(rows)
        refetch_count = len(rows_to_refetch)
        print(f"Found {refetch_count} rows that need date refetching (missing or today's date: {today_str}) out of {total_rows} total rows.")
        
        if refetch_count == 0:
            print("No dates need refetching. All dates are already set and not today's date.")
            # Still write output file (with same content)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            input_path = Path(args.input_csv)
            output_filename = f"{input_path.stem}_refetched_{timestamp}{input_path.suffix}"
            with open(output_filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"Output CSV written to: {output_filename}")
            return
        
        print(f"Fetching Set 2 commit dates for {refetch_count} CCD codes...")
        
        # Fetch dates in batches (only for rows that need refetching)
        set2_dates_cache = {}
        batch_size = 50
        filenames = list(ccd_to_filename.values())
        for i in tqdm(range(0, len(filenames), batch_size), desc="Fetching dates", unit="batch"):
            batch = filenames[i:i+batch_size]
            batch_results = get_github_commit_dates_batch(batch, "https://github.com/MonomerLibrary/monomers", github_token)
            set2_dates_cache.update(batch_results)
        
        fetched_count = len([d for d in set2_dates_cache.values() if d])
        total_requested = len(filenames)
        print(f"Fetched {fetched_count} dates from batch API (out of {total_requested} requested).")
        
        # If batch fetching returned very few dates or missed many files, fall back to individual API calls
        # This handles cases where batch fetching fails (rate limits, etc.)
        missing_count = total_requested - fetched_count
        if missing_count > 0:
            # If we're missing more than 10% of dates, try individual API calls for the missing ones
            if fetched_count == 0 or (missing_count > total_requested * 0.1):
                if fetched_count == 0:
                    print("Batch fetching returned 0 dates. Falling back to individual API calls...")
                else:
                    print(f"Batch fetching missed {missing_count} dates ({missing_count/total_requested*100:.1f}%). Falling back to individual API calls for missing dates...")
                print("Note: This will be slower but should work even if batch API has issues.")
                
                # Get list of filenames that need fetching (those with None or missing from cache)
                missing_filenames = [f for f in filenames if f not in set2_dates_cache or set2_dates_cache[f] is None]
                
                # Try a small sample first to see if individual calls work
                sample_size = min(10, len(missing_filenames))
                sample_fetched = 0
                sample_not_found = 0
                sample_errors = []
                
                for filename in missing_filenames[:sample_size]:
                    try:
                        date = get_github_commit_date(filename, "https://github.com/MonomerLibrary/monomers", github_token)
                        if date:
                            set2_dates_cache[filename] = date
                            sample_fetched += 1
                        else:
                            # None result - file might not exist (this is OK, not an error)
                            sample_not_found += 1
                    except Exception as e:
                        # Actual API error (rate limit, network, etc.)
                        sample_errors.append(f"{filename}: {str(e)}")
                
                # If we got any successful fetches OR no actual errors (just files not found), continue
                # Files not found is expected and not a failure
                if sample_fetched > 0 or (sample_errors == [] and sample_not_found > 0):
                    if sample_fetched > 0:
                        print(f"Individual API calls working ({sample_fetched}/{sample_size} sample succeeded, {sample_not_found} files not found). Fetching remaining {len(missing_filenames)} missing files...")
                    else:
                        print(f"Individual API calls working ({sample_not_found}/{sample_size} files checked, not found in repository - this is expected). Fetching remaining {len(missing_filenames)} missing files...")
                    
                    for filename in tqdm(missing_filenames, desc="Fetching missing dates individually", unit="file"):
                        if filename not in set2_dates_cache or set2_dates_cache[filename] is None:
                            date = get_github_commit_date(filename, "https://github.com/MonomerLibrary/monomers", github_token)
                            if date:
                                set2_dates_cache[filename] = date
                else:
                    # Only report failure if there were actual API errors
                    print("Warning: Individual API calls encountered errors. Dates may not be updated.")
                    if sample_errors:
                        print(f"Sample errors (first 3): {sample_errors[:3]}")
                    # Check if it's a rate limit issue
                    if github_token:
                        print("Note: GitHub token is being used. If rate limits are still hit, the token may need to be refreshed.")
                    else:
                        print("Note: No GitHub token provided. Consider using --github-token for higher rate limits.")
        
        final_fetched = len([d for d in set2_dates_cache.values() if d])
        print(f"Total fetched: {final_fetched} dates.")
        
        # Update rows with new dates (only for rows that needed refetching)
        updated_count = 0
        not_found_count = 0
        none_date_count = 0
        for ccd_code in rows_to_refetch:
            if ccd_code in ccd_to_row:
                row = ccd_to_row[ccd_code]
                filename = ccd_to_filename.get(ccd_code)
                if filename and filename in set2_dates_cache:
                    new_date = set2_dates_cache[filename]
                    old_date = row.get('ccp4_modified_date', '').strip()
                    # Only update if we have a valid (non-None, non-empty) date
                    if new_date:
                        row['ccp4_modified_date'] = new_date
                        if new_date != old_date:
                            updated_count += 1
                        else:
                            updated_count += 1  # Still count as updated for consistency
                    else:
                        none_date_count += 1
                else:
                    not_found_count += 1
        
        print(f"Updated {updated_count} dates.")
        if not_found_count > 0:
            print(f"Warning: {not_found_count} rows had no matching filename in cache.")
        if none_date_count > 0:
            print(f"Warning: {none_date_count} rows had None/empty dates in cache (API may have failed for these files).")
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        input_path = Path(args.input_csv)
        output_filename = f"{input_path.stem}_refetched_{timestamp}{input_path.suffix}"
        
        # Write updated CSV
        with open(output_filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"Updated CSV written to: {output_filename}")
        return
    
    # Validate correlation table is provided when needed
    if not args.download_only and not args.correlation_table:
        parser.error("--correlation-table is required when comparing files. Use --download-only to skip comparison.")
    
    # Initialize comparison engine (will only be used if comparing)
    comparison_engine = ComparisonEngine(args.correlation_table) if not args.download_only else None
    
    # Track missing files for reporting
    missing_files = []  # List of dicts: {'ccd_code': str, 'missing_from_set1': bool, 'missing_from_set2': bool}
    
    # Get file lists
    if args.mode == 'local':
        # Use local example files - recursively find all .cif files
        def find_cif_files(root_dir):
            """Recursively find all .cif files in a directory."""
            cif_files = []
            for root, dirs, files in os.walk(root_dir):
                for file in files:
                    if file.endswith('.cif'):
                        # Get relative path from root_dir
                        rel_path = os.path.relpath(os.path.join(root, file), root_dir)
                        cif_files.append(rel_path)
            return cif_files
        
        set1_files = sorted(find_cif_files(args.set1_dir))
        set2_files = sorted(find_cif_files(args.set2_dir))
        
        # Match files by basename (filename only, not path)
        set1_names = {os.path.basename(f): f for f in set1_files}
        set2_names = {os.path.basename(f): f for f in set2_files}
        
        # Track missing files
        set1_codes = {os.path.splitext(os.path.basename(f))[0] for f in set1_files}
        set2_codes = {os.path.splitext(os.path.basename(f))[0] for f in set2_files}
        all_codes = set1_codes | set2_codes
        
        for code in sorted(all_codes):
            missing_from_set1 = code not in set1_codes
            missing_from_set2 = code not in set2_codes
            if missing_from_set1 or missing_from_set2:
                missing_files.append({
                    'ccd_code': code,
                    'missing_from_set1': missing_from_set1,
                    'missing_from_set2': missing_from_set2
                })
        
        # Match files by basename
        file_pairs = []
        for name in set1_names:
            if name in set2_names:
                file_pairs.append((
                    os.path.join(args.set1_dir, set1_names[name]),
                    os.path.join(args.set2_dir, set2_names[name])
                ))
    
    elif args.mode == 'download':
        # Determine what to download
        download_set1 = args.download_set1
        download_set2 = args.download_set2
        
        # If neither flag is set, download both (backward compatibility)
        if not download_set1 and not download_set2:
            download_set1 = True
            download_set2 = True
        
        set1_file_list = []
        set2_file_list = []
        # Try EBI first (simpler flat structure), fall back to wwpdb if needed
        ebi_base = "https://www.ebi.ac.uk/pdbe/static/files/pdbechem_v2/"
        http_base = "https://files.wwpdb.org/pub/pdb/refdata/chem_comp/"
        github_base = "https://github.com/MonomerLibrary/monomers/tree/master/"
        
            # Get file lists and download Set 1
        if download_set1:
            print("Getting file list from Set 1...")
            # Download and split the components.cif.gz file
            set1_file_list = FileDownloader.download_and_split_components(
                show_progress=True, 
                output_dir=args.set1_dir
            )
            print(f"\nFound {len(set1_file_list)} files in Set 1")
            # Files are already downloaded and split, no need to download again
        
        # Get file lists and download Set 2
        if download_set2:
            print("\nGetting file list from Set 2 (GitHub)...")
            set2_file_list = FileDownloader.get_github_file_list(github_base, show_progress=True, github_token=github_token)
            print(f"\nFound {len(set2_file_list)} files in Set 2")
            
            # Create download directory
            os.makedirs(args.set2_dir, exist_ok=True)
            
            # Download set2 files, preserving directory structure
            print("Downloading Set 2 files...")
            skipped = 0
            for file_path in tqdm(set2_file_list, desc="Downloading Set 2 (GitHub)", unit="file"):
                # Preserve directory structure: set2_dir/0/000.cif
                local_path = os.path.join(args.set2_dir, file_path)
                if os.path.exists(local_path):
                    skipped += 1
                FileDownloader.download_github_file(github_base, file_path, local_path)
            if skipped > 0:
                print(f"Skipped {skipped} existing files (resume support).")
        
        # If download-only mode, skip file matching
        if args.download_only:
            print("Download-only mode: Skipping file matching.")
            file_pairs = []
        else:
            # If we didn't download, we need to get file lists for matching
            if not download_set1:
                print("Getting file list from Set 1 (downloading and splitting components.cif.gz)...")
                # Use the same method as download mode - download and split the archive
                set1_file_list = FileDownloader.download_and_split_components(
                    show_progress=True, 
                    output_dir=args.set1_dir
                )
            if not download_set2:
                print("Getting file list from Set 2 (GitHub) for matching...")
                set2_file_list = FileDownloader.get_github_file_list(github_base, show_progress=False, github_token=github_token)
            
            # Match files by name (basename), preserving structure
            set1_names = {os.path.basename(f): f for f in set1_file_list}
            set2_names = {os.path.basename(f): f for f in set2_file_list}
            
            # Track missing files
            set1_codes = {os.path.splitext(os.path.basename(f))[0] for f in set1_file_list}
            set2_codes = {os.path.splitext(os.path.basename(f))[0] for f in set2_file_list}
            all_codes = set1_codes | set2_codes
            
            for code in sorted(all_codes):
                missing_from_set1 = code not in set1_codes
                missing_from_set2 = code not in set2_codes
                if missing_from_set1 or missing_from_set2:
                    missing_files.append({
                        'ccd_code': code,
                        'missing_from_set1': missing_from_set1,
                        'missing_from_set2': missing_from_set2
                    })
            
            file_pairs = []
            for name in set1_names:
                if name in set2_names:
                    # Find the actual file paths in the local directories
                    set1_remote_path = set1_names[name]
                    set2_remote_path = set2_names[name]
                    set1_local = os.path.join(args.set1_dir, set1_remote_path)
                    set2_local = os.path.join(args.set2_dir, set2_remote_path)
                    file_pairs.append((set1_local, set2_local))
    
    elif args.mode == 'online':
        # Compare files directly from remote sources without downloading
        http_base = "https://files.wwpdb.org/pub/pdb/refdata/chem_comp/"
        github_base = "https://github.com/MonomerLibrary/monomers/tree/master/"
        
        if args.ccd_codes:
            # If specific codes are provided, construct URLs directly
            ccd_codes = [code.strip() for code in args.ccd_codes.split(',')]
            print(f"Constructing URLs for CCD codes: {', '.join(ccd_codes)}")
            
            set1_file_list = []
            set2_file_list = []
            
            for code in ccd_codes:
                # Construct Set 1 path (HTTP): directory_char/full_code/filename.cif
                # For 3-char codes: Use last character as directory
                #   Examples: 0/000/000.cif, 1/001/001.cif
                # For 5-char codes: Use last character as directory  
                #   Examples: 5/A1A15/A1A15.cif
                if len(code) == 3:
                    # 3-letter code: use last character
                    set1_path = f"{code[-1]}/{code}/{code}.cif"
                elif len(code) == 5:
                    # 5-letter code: use last character
                    set1_path = f"{code[-1]}/{code}/{code}.cif"
                else:
                    print(f"Warning: Unsupported code length for {code}, skipping...")
                    continue
                
                # Construct Set 2 path (GitHub): first_char/filename.cif
                set2_path = f"{code[0]}/{code}.cif"
                
                # Verify files exist by trying to access them
                set1_url = http_base.rstrip('/') + '/' + set1_path
                set1_found = False
                try:
                    req = Request(set1_url)
                    req.add_header('User-Agent', 'Mozilla/5.0')
                    with urlopen(req, timeout=10) as response:
                        if response.getcode() == 200:
                            set1_file_list.append(set1_path)
                            set1_found = True
                except Exception as e:
                    # File doesn't exist or error accessing
                    pass
                
                # For Set 2, we'll try to access via GitHub API or raw URL
                set2_raw_url = github_base.replace('https://github.com/', 'https://raw.githubusercontent.com/')
                set2_raw_url = set2_raw_url.replace('/tree/master/', '/master/') + set2_path
                set2_found = False
                try:
                    req = Request(set2_raw_url)
                    req.add_header('User-Agent', 'Mozilla/5.0')
                    with urlopen(req, timeout=10) as response:
                        if response.getcode() == 200:
                            set2_file_list.append(set2_path)
                            set2_found = True
                except:
                    pass
                
                # Print status for each code
                status = []
                if set1_found:
                    status.append("Set1[OK]")
                else:
                    status.append("Set1[NOT FOUND]")
                if set2_found:
                    status.append("Set2[OK]")
                else:
                    status.append("Set2[NOT FOUND]")
                print(f"  {code}: {' '.join(status)}")
            
            print(f"\nFound {len(set1_file_list)} files in Set 1, {len(set2_file_list)} files in Set 2")
            
            # Report missing files
            set1_codes = {os.path.splitext(os.path.basename(f))[0] for f in set1_file_list}
            set2_codes = {os.path.splitext(os.path.basename(f))[0] for f in set2_file_list}
            requested_codes = set(ccd_codes)
            
            missing_from_set1 = requested_codes - set1_codes
            missing_from_set2 = requested_codes - set2_codes
            missing_from_both = missing_from_set1 & missing_from_set2
            
            # Track missing files for reporting
            for code in sorted(requested_codes):
                missing_from_s1 = code in missing_from_set1
                missing_from_s2 = code in missing_from_set2
                if missing_from_s1 or missing_from_s2:
                    missing_files.append({
                        'ccd_code': code,
                        'missing_from_set1': missing_from_s1,
                        'missing_from_set2': missing_from_s2
                    })
            
            if missing_from_set1 or missing_from_set2:
                print("\nMissing files:")
                if missing_from_set1 - missing_from_both:
                    print(f"  Missing from Set 1 only: {', '.join(sorted(missing_from_set1 - missing_from_both))}")
                if missing_from_set2 - missing_from_both:
                    print(f"  Missing from Set 2 only: {', '.join(sorted(missing_from_set2 - missing_from_both))}")
                if missing_from_both:
                    print(f"  Missing from both sets: {', '.join(sorted(missing_from_both))}")
                print("  (Only files present in both sets will be compared)")
        else:
            # Get all files (this is slow, but works)
            print("Getting file list from Set 1 (HTTP)...")
            set1_file_list = FileDownloader.download_and_split_components(show_progress=True, output_dir=args.set1_dir)
            
            print(f"\nFound {len(set1_file_list)} files in Set 1")
            print("\nGetting file list from Set 2 (GitHub)...")
            set2_file_list = FileDownloader.get_github_file_list(github_base, show_progress=True, github_token=github_token)
            
            print(f"\nFound {len(set2_file_list)} files in Set 2")
        
        # Match files by name (basename)
        set1_names = {os.path.basename(f): f for f in set1_file_list}
        set2_names = {os.path.basename(f): f for f in set2_file_list}
        
        # Track missing files (for online mode without --ccd-codes)
        if not args.ccd_codes:
            set1_codes = {os.path.splitext(os.path.basename(f))[0] for f in set1_file_list}
            set2_codes = {os.path.splitext(os.path.basename(f))[0] for f in set2_file_list}
            all_codes = set1_codes | set2_codes
            
            for code in sorted(all_codes):
                missing_from_set1 = code not in set1_codes
                missing_from_set2 = code not in set2_codes
                if missing_from_set1 or missing_from_set2:
                    missing_files.append({
                        'ccd_code': code,
                        'missing_from_set1': missing_from_set1,
                        'missing_from_set2': missing_from_set2
                    })
        
        file_pairs = []
        for name in set1_names:
            if name in set2_names:
                # Store remote paths as tuples (source_type, base_url, file_path)
                file_pairs.append((
                    ('http', http_base, set1_names[name]),
                    ('github', github_base, set2_names[name])
                ))
    
    # Check if we should skip comparison (download-only mode or no file pairs)
    skip_comparison = args.download_only or len(file_pairs) == 0
    
    if skip_comparison:
        if args.download_only:
            print("Download-only mode: Skipping comparison.")
            print("Download complete.")
        elif len(file_pairs) == 0:
            print("No matching file pairs found. Skipping comparison.")
            if args.mode == 'download':
                print("Note: If you only downloaded one set, use --download-only to skip comparison.")
                print("Download complete.")
            elif args.mode == 'local':
                print("Note: Make sure both set1_dir and set2_dir contain matching .cif files.")
        
        # Generate timestamp for output filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Add timestamp to output filename
        output_path = Path(args.output)
        output_with_timestamp = f"{output_path.stem}_{timestamp}{output_path.suffix}"
        
        # Still write missing files report if available
        if missing_files:
            missing_output = output_with_timestamp.replace('.csv', '_missing_files.csv')
            with open(missing_output, 'w', newline='') as f:
                fieldnames = ['ccd_code', 'missing_from_set1', 'missing_from_set2', 'missing_from_both']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for item in missing_files:
                    writer.writerow({
                        'ccd_code': item['ccd_code'],
                        'missing_from_set1': 'Y' if item['missing_from_set1'] else 'N',
                        'missing_from_set2': 'Y' if item['missing_from_set2'] else 'N',
                        'missing_from_both': 'Y' if (item['missing_from_set1'] and item['missing_from_set2']) else 'N'
                    })
            print(f"Missing files report written to {missing_output}")
            print(f"Total missing files tracked: {len(missing_files)}")
        
        return
    
    # Apply limit if specified (for testing)
    if args.limit and args.limit > 0:
        original_count = len(file_pairs)
        file_pairs = file_pairs[:args.limit]
        print(f"Limiting comparison to first {args.limit} file pairs (out of {original_count} total)...")
    
    print(f"Comparing {len(file_pairs)} file pairs...")
    
    # Pre-fetch Set 2 commit dates in batches for better performance
    set2_dates_cache = {}  # Dictionary to pass to workers: filename -> date
    batch_fetching_attempted = False  # Flag to track if batch fetching was attempted
    if args.mode in ['local', 'download']:
        batch_fetching_attempted = True
        print("Pre-fetching Set 2 commit dates from GitHub (batch API)...")
        set2_file_names = []
        for file1, file2 in file_pairs:
            if args.mode == 'online':
                _, _, path2 = file2
                set2_file_names.append(os.path.basename(path2))
            else:
                set2_file_names.append(os.path.basename(file2))
        
        unique_file_names = list(set(set2_file_names))
        print(f"  Fetching dates for {len(unique_file_names)} unique files...")
        
        # Fetch dates in batches
        batch_size = 50
        for i in tqdm(range(0, len(unique_file_names), batch_size), desc="Fetching dates", unit="batch"):
            batch = unique_file_names[i:i+batch_size]
            batch_results = get_github_commit_dates_batch(batch, "https://github.com/MonomerLibrary/monomers", github_token)
            # Store results in cache dictionary
            set2_dates_cache.update(batch_results)
        print(f"  Batch fetching complete. Fetched {len([d for d in set2_dates_cache.values() if d])} dates.")
    
    # Determine number of worker processes (use CPU count, but cap at reasonable limit)
    num_workers = min(cpu_count(), 8, len(file_pairs))  # Use up to 8 cores, or number of pairs if less
    use_parallel = num_workers > 1 and len(file_pairs) > 10  # Only use parallel if we have multiple cores and enough work
    
    if use_parallel:
        print(f"Using {num_workers} parallel workers for comparison...")
    
    # Perform comparisons (parallel or sequential)
    results = []
    
    if use_parallel:
        # Prepare arguments for workers (include pre-fetched dates cache and batch fetching flag)
        worker_args = [(pair, args.mode, args.correlation_table, github_token, set2_dates_cache, batch_fetching_attempted) for pair in file_pairs]
        
        # Process in parallel
        with Pool(processes=num_workers) as pool:
            results = list(tqdm(
                pool.imap(compare_file_pair_worker, worker_args),
                total=len(file_pairs),
                desc="Comparing files",
                unit="pair"
            ))
        
        # Filter out None results (failed comparisons)
        results = [r for r in results if r is not None]
    else:
        # Sequential processing (original code)
        for file1, file2 in tqdm(file_pairs, desc="Comparing files", unit="pair"):
            try:
                # Handle online mode (file1 and file2 are tuples)
                if args.mode == 'online':
                    source1_type, base1, path1 = file1
                    source2_type, base2, path2 = file2
                    
                    # Fetch content from remote sources
                    if source1_type == 'http':
                        content1 = FileDownloader.get_http_file_content(base1, path1)
                    else:
                        content1 = FileDownloader.get_github_file_content(base1, path1)
                    
                    if source2_type == 'http':
                        content2 = FileDownloader.get_http_file_content(base2, path2)
                    else:
                        content2 = FileDownloader.get_github_file_content(base2, path2)
                    
                    if content1 is None or content2 is None:
                        print(f"Warning: Could not fetch content for {path1} or {path2}, skipping...")
                        continue
                    
                    parser1 = mmCIFParser(content=content1)
                    parser2 = mmCIFParser(content=content2)
                    file_name = get_file_name_from_path(path1)
                else:
                    # Local mode - use file paths
                    parser1 = mmCIFParser(file1)
                    parser2 = mmCIFParser(file2)
                    file_name = get_file_name_from_path(file1)
                    path2 = file2  # For date retrieval
                
                comparison_results = comparison_engine.compare_all(parser1, parser2)
                
                # Extract individual comparison results using new keys
                name_match = comparison_results.get('name', False)
                type_match = comparison_results.get('type', False)
                atom_match = comparison_results.get('atom', False)
                bond_match = comparison_results.get('bond', False)
                descriptor_match = comparison_results.get('descriptor', False)
                
                overall_match = 'Y' if all([name_match, type_match, atom_match, bond_match, descriptor_match]) else 'N'
                
                if args.mode == 'online':
                    # For online mode, dates come from parser/API only
                    set1_date = get_modified_date(parser1, None, use_file_date=False)  # Use parser only for set1
                    file2_name = os.path.basename(path2)
                    # Try cache first - if batch fetching was attempted, don't make individual API calls
                    set2_date = None
                    if batch_fetching_attempted:
                        # Batch fetching was attempted, only use cache
                        set2_date = set2_dates_cache.get(file2_name)
                        # Don't make individual API calls if cache lookup fails - batch fetching already tried
                    else:
                        # No batch fetching attempted, make individual API call
                        set2_date = get_modified_date(parser2, None, use_file_date=True, 
                                                     file_name=file2_name, 
                                                     repo_url="https://github.com/MonomerLibrary/monomers",
                                                     github_token=github_token,
                                                     use_file_fallback=False)
                else:
                    # For local and download modes, use GitHub commit date for set2
                    set1_date = get_modified_date(parser1, file1, use_file_date=False)  # Use parser only for set1
                    file2_name = os.path.basename(file2)
                    # Try cache first - if batch fetching was attempted, don't make individual API calls
                    set2_date = None
                    if batch_fetching_attempted:
                        # Batch fetching was attempted, only use cache
                        set2_date = set2_dates_cache.get(file2_name)
                        # Don't make individual API calls if cache lookup fails - batch fetching already tried
                    else:
                        # No batch fetching attempted, make individual API call
                        set2_date = get_modified_date(parser2, file2, use_file_date=True, 
                                                     file_name=file2_name, 
                                                     repo_url="https://github.com/MonomerLibrary/monomers",
                                                     github_token=github_token,
                                                     use_file_fallback=False)
                
                results.append({
                    'ccd_code': file_name,
                    'name_identical': 'Y' if name_match else 'N',
                    'type_identical': 'Y' if type_match else 'N',
                    'atom_identical': 'Y' if atom_match else 'N',
                    'bond_identical': 'Y' if bond_match else 'N',
                    'descriptor_identical': 'Y' if descriptor_match else 'N',
                    'overall_identical': overall_match,
                    'wwpdb_modified_date': set1_date or '',
                    'ccp4_modified_date': set2_date or ''
                })
            except Exception as e:
                print(f"Error processing {file1} and {file2}: {e}")
                import traceback
                traceback.print_exc()
                # Handle both file paths and tuples (for online mode)
                if isinstance(file1, tuple):
                    file_name = get_file_name_from_path(file1[2])  # path is third element
                else:
                    file_name = get_file_name_from_path(file1)
                results.append({
                    'ccd_code': file_name,
                    'name_identical': 'N',
                    'type_identical': 'N',
                    'atom_identical': 'N',
                    'bond_identical': 'N',
                    'descriptor_identical': 'N',
                    'overall_identical': 'N',
                    'wwpdb_modified_date': '',
                    'ccp4_modified_date': ''
                })
    
    # Generate timestamp for output filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Add timestamp to output filename
    output_path = Path(args.output)
    output_with_timestamp = f"{output_path.stem}_{timestamp}{output_path.suffix}"
    
    # Write results to CSV
    fieldnames = [
        'ccd_code', 'name_identical', 'type_identical', 'atom_identical',
        'bond_identical', 'descriptor_identical', 'overall_identical',
        'wwpdb_modified_date', 'ccp4_modified_date'
    ]
    
    with open(output_with_timestamp, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Results written to {output_with_timestamp}")
    print(f"Total files compared: {len(results)}")
    
    # Write missing files report
    if missing_files:
        missing_output = output_with_timestamp.replace('.csv', '_missing_files.csv')
        with open(missing_output, 'w', newline='') as f:
            fieldnames = ['ccd_code', 'missing_from_set1', 'missing_from_set2', 'missing_from_both']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in missing_files:
                writer.writerow({
                    'ccd_code': item['ccd_code'],
                    'missing_from_set1': 'Y' if item['missing_from_set1'] else 'N',
                    'missing_from_set2': 'Y' if item['missing_from_set2'] else 'N',
                    'missing_from_both': 'Y' if (item['missing_from_set1'] and item['missing_from_set2']) else 'N'
                })
        print(f"Missing files report written to {missing_output}")
        print(f"Total missing files tracked: {len(missing_files)}")
    else:
        print("No missing files found - all requested files are present in both sets.")


if __name__ == '__main__':
    main()


