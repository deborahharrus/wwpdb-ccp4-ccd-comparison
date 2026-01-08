#!/usr/bin/env python3
"""
Create a detailed comparison CSV with actual values for differences.

This script reads a comparison results CSV and creates an enhanced version
with additional columns showing the actual values from Set 1 and Set 2
when differences are detected.

Performance optimizations:
- File path cache: Pre-scans directories to build a lookup table
- Parallel processing: Uses multiple CPU cores to process rows concurrently
- Conditional extraction: Only extracts data types that differ (atoms/bonds/descriptors)
- Resume capability: Can resume from checkpoints if interrupted
"""

import csv
import os
import re
import sys
import json
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from multiprocessing import Pool, cpu_count, Manager
from functools import partial

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Fallback: create a dummy tqdm that does nothing
    class tqdm:
        def __init__(self, *args, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def update(self, n=1):
            pass

# Import the mmCIF parser from ccd_sync
from ccd_sync import mmCIFParser


# Global caches
_file_path_cache = {}  # {ccd_code: (set1_path, set2_path)}
_parsed_file_cache = {}  # {file_path: mmCIFParser}


def build_file_path_cache(set1_dir: str, set2_dir: str, cache_file: str = None) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """Build a cache of all CIF file paths by scanning directories once.
    
    Returns a dictionary mapping CCD codes to (set1_path, set2_path) tuples.
    """
    global _file_path_cache
    
    # Try to load from cache file if it exists
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                _file_path_cache = json.load(f)
            print(f"Loaded file path cache from {cache_file} ({len(_file_path_cache)} entries)")
            return _file_path_cache
        except Exception as e:
            print(f"Warning: Could not load cache file: {e}. Rebuilding cache...")
    
    print("Building file path cache (this may take a few minutes for large directories)...")
    
    # Scan Set 1 directory
    set1_files = {}
    if os.path.exists(set1_dir):
        print(f"  Scanning Set 1 directory: {set1_dir}")
        for root, dirs, files in os.walk(set1_dir):
            for file in files:
                if file.endswith('.cif') and len(file) >= 8:  # At least "XXX.cif"
                    ccd_code = file[:-4].upper()  # Remove .cif extension
                    full_path = os.path.join(root, file)
                    # Store the first found path for each code
                    if ccd_code not in set1_files:
                        set1_files[ccd_code] = full_path
    
    # Scan Set 2 directory
    set2_files = {}
    if os.path.exists(set2_dir):
        print(f"  Scanning Set 2 directory: {set2_dir}")
        for root, dirs, files in os.walk(set2_dir):
            for file in files:
                if file.endswith('.cif') and len(file) >= 8:  # At least "XXX.cif"
                    ccd_code = file[:-4].upper()  # Remove .cif extension
                    full_path = os.path.join(root, file)
                    # Store the first found path for each code
                    if ccd_code not in set2_files:
                        set2_files[ccd_code] = full_path
    
    # Combine into cache
    all_codes = set(set1_files.keys()) | set(set2_files.keys())
    _file_path_cache = {
        code: (set1_files.get(code), set2_files.get(code))
        for code in all_codes
    }
    
    print(f"  Found {len(set1_files)} Set 1 files, {len(set2_files)} Set 2 files")
    print(f"  Cache built: {len(_file_path_cache)} total CCD codes")
    
    # Save cache to file
    if cache_file:
        try:
            with open(cache_file, 'w') as f:
                json.dump(_file_path_cache, f, indent=2)
            print(f"  Cache saved to {cache_file}")
        except Exception as e:
            print(f"  Warning: Could not save cache file: {e}")
    
    return _file_path_cache


def find_cif_file_from_cache(ccd_code: str, set1_dir: str, set2_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """Find CIF files for a given CCD code using the cache.
    Returns (set1_file, set2_file) tuple.
    """
    global _file_path_cache
    
    ccd_code_upper = ccd_code.upper()
    if ccd_code_upper in _file_path_cache:
        set1_path, set2_path = _file_path_cache[ccd_code_upper]
        # Verify files still exist and match the expected directories
        if set1_path:
            # Normalize paths for comparison
            set1_path_norm = os.path.normpath(set1_path)
            set1_dir_norm = os.path.normpath(set1_dir)
            if not (os.path.exists(set1_path) and set1_path_norm.startswith(set1_dir_norm)):
                set1_path = None
        if set2_path:
            set2_path_norm = os.path.normpath(set2_path)
            set2_dir_norm = os.path.normpath(set2_dir)
            if not (os.path.exists(set2_path) and set2_path_norm.startswith(set2_dir_norm)):
                set2_path = None
        return (set1_path, set2_path)
    
    # Not in cache, return None (caller can fall back to search if needed)
    return (None, None)


def find_cif_file(ccd_code: str, base_dir: str) -> Optional[str]:
    """Find the CIF file for a given CCD code (fallback search when not in cache)."""
    # Try common paths (fast check)
    if len(ccd_code) >= 3:
        last_char = ccd_code[-1]
        possible_paths = [
            os.path.join(base_dir, last_char, ccd_code, f"{ccd_code}.cif"),
            os.path.join(base_dir, last_char, f"{ccd_code}.cif"),
            os.path.join(base_dir, ccd_code, f"{ccd_code}.cif"),
            os.path.join(base_dir, f"{ccd_code}.cif"),
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
    
    # Last resort: recursive search (slow, but should rarely be needed if cache is built)
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == f"{ccd_code}.cif":
                return os.path.join(root, file)
    
    return None


def get_parser(file_path: Optional[str]) -> Optional[mmCIFParser]:
    """Get a parser for a CIF file, using cache if available."""
    global _parsed_file_cache
    
    if not file_path or not os.path.exists(file_path):
        return None
    
    # Check cache first
    if file_path in _parsed_file_cache:
        return _parsed_file_cache[file_path]
    
    # Parse and cache
    try:
        parser = mmCIFParser(file_path)
        _parsed_file_cache[file_path] = parser
        return parser
    except Exception as e:
        if HAS_TQDM:
            tqdm.write(f"Warning: Error parsing {file_path}: {e}")
        else:
            print(f"Warning: Error parsing {file_path}: {e}")
        return None


def format_value(value: Any) -> str:
    """Format a value for CSV output."""
    if value is None:
        return ""
    if isinstance(value, list):
        if len(value) == 0:
            return ""
        # For lists, format as a readable string
        return "; ".join(str(v) for v in value[:10]) + (" ..." if len(value) > 10 else "")
    # Remove newlines for CSV output (newlines are formatting artifacts, not actual content)
    # This ensures multi-line values are displayed correctly without introducing spaces
    result = str(value)
    # Remove various newline characters (don't replace with spaces)
    result = result.replace('\r\n', '').replace('\n', '').replace('\r', '')
    return result.strip()


def get_field_value(parser: mmCIFParser, field: str) -> Optional[Any]:
    """Get a field value from the parser."""
    try:
        # Try direct value first (with underscore)
        value = parser.get_value(field)
        if value is not None and value != '':
            return value
        
        # Try without leading underscore (for multi-line values stored without underscore)
        field_no_underscore = field.lstrip('_')
        if field_no_underscore != field:
            value = parser.get_value(field_no_underscore)
            if value is not None and value != '':
                return value
        
        # For Set 2, some fields like _chem_comp.name, _chem_comp.group might be in a loop
        # Check if it's in the comp_list loop
        if field.startswith('_chem_comp.'):
            comp_list = parser.get_loop_data('chem_comp')
            if comp_list and len(comp_list) > 0:
                # Try exact field name first
                if field in comp_list[0]:
                    return comp_list[0].get(field)
                # Try without leading underscore
                if field_no_underscore in comp_list[0]:
                    return comp_list[0].get(field_no_underscore)
                # For backward compatibility, check for 'group' field specifically
                if field == '_chem_comp.group':
                    for key in comp_list[0].keys():
                        if 'group' in key.lower():
                            return comp_list[0].get(key)
        
        return None
    except:
        return None


def get_loop_values(parser: mmCIFParser, category: str) -> List[Dict[str, Any]]:
    """Get all loop values for a category."""
    try:
        # Remove leading underscore if present, get_loop_data handles it
        cat = category.lstrip('_')
        return parser.get_loop_data(cat)
    except:
        return []


def get_atom_key(atom_dict, key_name):
    """Helper to get atom key from dict with different possible key formats."""
    for possible_key in [key_name, f'_{key_name}', key_name.split('.')[-1]]:
        if possible_key in atom_dict:
            return atom_dict[possible_key]
    return '?'


def atom_to_tuple(atom: Dict[str, Any]) -> tuple:
    """Convert atom dict to tuple for comparison."""
    atom_id = get_atom_key(atom, '_chem_comp_atom.atom_id')
    type_symbol = get_atom_key(atom, '_chem_comp_atom.type_symbol')
    charge = get_atom_key(atom, '_chem_comp_atom.charge')
    return (atom_id, type_symbol, charge)


def format_atom_differences(atoms1: List[Dict[str, Any]], atoms2: List[Dict[str, Any]]) -> tuple:
    """Format only the differing atoms between two sets.
    Returns (set1_differences, set2_differences) as formatted strings.
    """
    # Convert to sets of tuples for comparison
    set1_atoms = {atom_to_tuple(a) for a in atoms1}
    set2_atoms = {atom_to_tuple(a) for a in atoms2}
    
    # Find differences
    only_in_set1 = set1_atoms - set2_atoms
    only_in_set2 = set2_atoms - set1_atoms
    
    # Format Set1 differences
    formatted1 = []
    for atom_tuple in sorted(only_in_set1):
        atom_id, type_symbol, charge = atom_tuple
        formatted1.append(f"{atom_id}({type_symbol},{charge})")
    
    # Format Set2 differences
    formatted2 = []
    for atom_tuple in sorted(only_in_set2):
        atom_id, type_symbol, charge = atom_tuple
        formatted2.append(f"{atom_id}({type_symbol},{charge})")
    
    return ("; ".join(formatted1), "; ".join(formatted2))


def get_bond_key(bond_dict, key_name):
    """Helper to get bond key from dict with different possible key formats."""
    # Try exact match first
    if key_name in bond_dict:
        return bond_dict[key_name]
    
    # Try without leading underscore
    key_no_underscore = key_name.lstrip('_')
    if key_no_underscore in bond_dict:
        return bond_dict[key_no_underscore]
    
    # Try just the field name (last part after dot)
    field_name = key_name.split('.')[-1]
    if field_name in bond_dict:
        return bond_dict[field_name]
    
    # For set2 bonds, if looking for 'type' but not found, try 'value_order' as fallback
    if 'type' in key_name.lower() and 'value_order' not in key_name.lower():
        # Try value_order as alternative
        alt_key = key_name.replace('.type', '.value_order')
        if alt_key in bond_dict:
            return bond_dict[alt_key]
        alt_key_no_underscore = alt_key.lstrip('_')
        if alt_key_no_underscore in bond_dict:
            return bond_dict[alt_key_no_underscore]
    
    # For set1 bonds, if looking for 'value_order' but not found, try 'type' as fallback
    if 'value_order' in key_name.lower() and 'type' not in key_name.lower():
        # Try type as alternative
        alt_key = key_name.replace('.value_order', '.type')
        if alt_key in bond_dict:
            return bond_dict[alt_key]
        alt_key_no_underscore = alt_key.lstrip('_')
        if alt_key_no_underscore in bond_dict:
            return bond_dict[alt_key_no_underscore]
    
    return '?'


def normalize_bond_order(order: str) -> str:
    """Normalize bond order for comparison (SING/SINGLE, DOUB/DOUBLE)."""
    order_upper = str(order).upper()
    if order_upper in ['SING', 'SINGLE']:
        return 'SING'
    elif order_upper in ['DOUB', 'DOUBLE']:
        return 'DOUB'
    return order_upper


def bond_to_tuple(bond: Dict[str, Any], set1: bool) -> tuple:
    """Convert bond dict to tuple for comparison."""
    atom1 = get_bond_key(bond, '_chem_comp_bond.atom_id_1')
    atom2 = get_bond_key(bond, '_chem_comp_bond.atom_id_2')
    
    # Normalize atom order (bonds are bidirectional)
    if atom1 > atom2:
        atom1, atom2 = atom2, atom1
    
    if set1:
        order = normalize_bond_order(get_bond_key(bond, '_chem_comp_bond.value_order'))
        aromatic = get_bond_key(bond, '_chem_comp_bond.pdbx_aromatic_flag')
    else:
        order = normalize_bond_order(get_bond_key(bond, '_chem_comp_bond.type'))
        aromatic = get_bond_key(bond, '_chem_comp_bond.aromatic')
    
    aromatic_str = "aromatic" if str(aromatic).upper() in ['Y', 'YES', 'TRUE', 'Y'] else "non-aromatic"
    return (atom1, atom2, order, aromatic_str)


def format_bond_differences(bonds1: List[Dict[str, Any]], bonds2: List[Dict[str, Any]]) -> tuple:
    """Format only the differing bonds between two sets.
    Returns (set1_differences, set2_differences) as formatted strings.
    """
    # Create mapping from normalized tuple to original bond dict for display
    set1_bond_map = {}
    set2_bond_map = {}
    
    for b in bonds1:
        normalized = bond_to_tuple(b, set1=True)
        # Store original atom order for display
        atom1_orig = get_bond_key(b, '_chem_comp_bond.atom_id_1')
        atom2_orig = get_bond_key(b, '_chem_comp_bond.atom_id_2')
        order_orig = get_bond_key(b, '_chem_comp_bond.value_order')
        aromatic_orig = get_bond_key(b, '_chem_comp_bond.pdbx_aromatic_flag')
        aromatic_str = "aromatic" if str(aromatic_orig).upper() in ['Y', 'YES', 'TRUE', 'Y'] else "non-aromatic"
        set1_bond_map[normalized] = (atom1_orig, atom2_orig, order_orig, aromatic_str)
    
    for b in bonds2:
        normalized = bond_to_tuple(b, set1=False)
        # Store original atom order for display
        atom1_orig = get_bond_key(b, '_chem_comp_bond.atom_id_1')
        atom2_orig = get_bond_key(b, '_chem_comp_bond.atom_id_2')
        order_orig = get_bond_key(b, '_chem_comp_bond.type')
        aromatic_orig = get_bond_key(b, '_chem_comp_bond.aromatic')
        aromatic_str = "aromatic" if str(aromatic_orig).upper() in ['Y', 'YES', 'TRUE', 'Y'] else "non-aromatic"
        set2_bond_map[normalized] = (atom1_orig, atom2_orig, order_orig, aromatic_str)
    
    # Find differences using normalized tuples
    set1_bonds = set(set1_bond_map.keys())
    set2_bonds = set(set2_bond_map.keys())
    
    only_in_set1 = set1_bonds - set2_bonds
    only_in_set2 = set2_bonds - set1_bonds
    
    # Format Set1 differences using normalized atom order (bonds are bidirectional)
    formatted1 = []
    for bond_tuple in sorted(only_in_set1):
        # Get original values from map for display
        atom1_orig, atom2_orig, order_orig, aromatic_str = set1_bond_map[bond_tuple]
        # Normalize atom order for display (ensure consistent ordering)
        if atom1_orig > atom2_orig:
            atom1_orig, atom2_orig = atom2_orig, atom1_orig
        # Normalize order for display (SING/SINGLE -> SING, DOUB/DOUBLE -> DOUB)
        order_display = normalize_bond_order(order_orig)
        formatted1.append(f"{atom1_orig}-{atom2_orig}({order_display},{aromatic_str})")
    
    # Format Set2 differences using normalized atom order (bonds are bidirectional)
    formatted2 = []
    for bond_tuple in sorted(only_in_set2):
        # Get original values from map for display
        atom1_orig, atom2_orig, order_orig, aromatic_str = set2_bond_map[bond_tuple]
        # Normalize atom order for display (ensure consistent ordering)
        if atom1_orig > atom2_orig:
            atom1_orig, atom2_orig = atom2_orig, atom1_orig
        # Normalize order for display (SING/SINGLE -> SING, DOUB/DOUBLE -> DOUB)
        order_display = normalize_bond_order(order_orig)
        formatted2.append(f"{atom1_orig}-{atom2_orig}({order_display},{aromatic_str})")
    
    return ("; ".join(formatted1), "; ".join(formatted2))


def descriptor_to_tuple(desc: Dict[str, Any]) -> tuple:
    """Convert descriptor dict to tuple for comparison."""
    desc_type = desc.get('_pdbx_chem_comp_descriptor.type', desc.get('type', '?'))
    program = desc.get('_pdbx_chem_comp_descriptor.program', desc.get('program', '?'))
    version = desc.get('_pdbx_chem_comp_descriptor.program_version', desc.get('program_version', '?'))
    descriptor = desc.get('_pdbx_chem_comp_descriptor.descriptor', desc.get('descriptor', '?'))
    return (desc_type, program, version, descriptor)


def format_descriptor_differences(desc1: List[Dict[str, Any]], desc2: List[Dict[str, Any]]) -> tuple:
    """Format only the differing descriptors between two sets.
    Returns (set1_differences, set2_differences) as formatted strings.
    """
    # Convert to sets of tuples for comparison
    set1_desc = {descriptor_to_tuple(d) for d in desc1}
    set2_desc = {descriptor_to_tuple(d) for d in desc2}
    
    # Find differences
    only_in_set1 = set1_desc - set2_desc
    only_in_set2 = set2_desc - set1_desc
    
    # Format Set1 differences
    formatted1 = []
    for desc_tuple in sorted(only_in_set1):
        desc_type, program, version, descriptor = desc_tuple
        # Truncate long descriptors
        if len(descriptor) > 50:
            descriptor = descriptor[:47] + "..."
        formatted1.append(f"{desc_type}({program} {version}): {descriptor}")
    
    # Format Set2 differences
    formatted2 = []
    for desc_tuple in sorted(only_in_set2):
        desc_type, program, version, descriptor = desc_tuple
        # Truncate long descriptors
        if len(descriptor) > 50:
            descriptor = descriptor[:47] + "..."
        formatted2.append(f"{desc_type}({program} {version}): {descriptor}")
    
    return ("; ".join(formatted1), "; ".join(formatted2))


def extract_values(parser: mmCIFParser, correlation_fields: List[Tuple[str, str]], needs_atoms: bool = False, needs_bonds: bool = False, needs_descriptors: bool = False) -> Dict[str, Any]:
    """Extract values for all correlation fields.
    
    Args:
        parser: The mmCIFParser instance
        correlation_fields: List of field correlations
        needs_atoms: Only extract atoms if True (optimization)
        needs_bonds: Only extract bonds if True (optimization)
        needs_descriptors: Only extract descriptors if True (optimization)
    """
    values = {}
    
    # Extract grouped fields only if needed (optimization)
    if needs_atoms and any(f.startswith('_chem_comp_atom.') for f, _ in correlation_fields):
        atoms = get_loop_values(parser, 'chem_comp_atom')
        values['_chem_comp_atom_set'] = atoms
    
    if needs_bonds and any(f.startswith('_chem_comp_bond.') for f, _ in correlation_fields):
        bonds = get_loop_values(parser, 'chem_comp_bond')
        values['_chem_comp_bond_set'] = bonds
    
    if needs_descriptors and any(f.startswith('_pdbx_chem_comp_descriptor.') for f, _ in correlation_fields):
        descriptors = get_loop_values(parser, 'pdbx_chem_comp_descriptor')
        values['_pdbx_chem_comp_descriptor_set'] = descriptors
    
    return values


def process_row(args: Tuple[Dict, str, str, str, str, List, Dict]) -> Dict:
    """Process a single row. Designed for parallel processing.
    
    Args:
        args: Tuple of (row, set1_dir, set2_dir, correlation_fields, file_path_cache)
    Returns:
        Processed detailed row
    """
    row, set1_dir, set2_dir, correlation_fields, file_path_cache = args
    
    ccd_code = row['ccd_code']
    detailed_row = row.copy()
    
    # Check if there are any differences
    has_differences = (
        row.get('name_identical', 'Y') == 'N' or
        row.get('type_identical', 'Y') == 'N' or
        row.get('atom_identical', 'Y') == 'N' or
        row.get('bond_identical', 'Y') == 'N' or
        row.get('descriptor_identical', 'Y') == 'N'
    )
    
    if not has_differences:
        return detailed_row
    
    # Find CIF files using cache
    ccd_code_upper = ccd_code.upper()
    set1_file, set2_file = None, None
    
    if file_path_cache and ccd_code_upper in file_path_cache:
        cache_entry = file_path_cache[ccd_code_upper]
        if isinstance(cache_entry, (list, tuple)) and len(cache_entry) >= 2:
            set1_file, set2_file = cache_entry[0], cache_entry[1]
        # Verify files exist
        if set1_file and not os.path.exists(set1_file):
            set1_file = None
        if set2_file and not os.path.exists(set2_file):
            set2_file = None
    
    # Fallback to search if not in cache
    # Try common path patterns for both 3-char and 5-char codes
    if not set1_file:
        if len(ccd_code) == 3:
            # 3-char code: try last_char/code/code.cif and last_char/first_two/code.cif
            last_char = ccd_code[-1]
            first_two = ccd_code[:2]
            for path_template in [
                os.path.join(set1_dir, last_char, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set1_dir, last_char, first_two, f"{ccd_code}.cif"),
                os.path.join(set1_dir, last_char, f"{ccd_code}.cif"),
                os.path.join(set1_dir, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set1_dir, f"{ccd_code}.cif"),
            ]:
                if os.path.exists(path_template):
                    set1_file = path_template
                    break
        elif len(ccd_code) == 5:
            # 5-char code: try last_char/code/code.cif
            last_char = ccd_code[-1]
            for path_template in [
                os.path.join(set1_dir, last_char, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set1_dir, last_char, f"{ccd_code}.cif"),
                os.path.join(set1_dir, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set1_dir, f"{ccd_code}.cif"),
            ]:
                if os.path.exists(path_template):
                    set1_file = path_template
                    break
        else:
            # Try root directory
            root_path = os.path.join(set1_dir, f"{ccd_code}.cif")
            if os.path.exists(root_path):
                set1_file = root_path
    
    if not set2_file:
        if len(ccd_code) >= 3:
            # Set2 often uses first_char/code.cif pattern
            first_char = ccd_code[0]
            last_char = ccd_code[-1]
            for path_template in [
                os.path.join(set2_dir, first_char, f"{ccd_code}.cif"),
                os.path.join(set2_dir, last_char, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set2_dir, last_char, f"{ccd_code}.cif"),
                os.path.join(set2_dir, ccd_code, f"{ccd_code}.cif"),
                os.path.join(set2_dir, f"{ccd_code}.cif"),
            ]:
                if os.path.exists(path_template):
                    set2_file = path_template
                    break
    
    if not set1_file or not set2_file:
        return detailed_row
    
    try:
        # Parse files (no caching in parallel mode - each process has its own memory)
        parser1 = mmCIFParser(set1_file)
        parser2 = mmCIFParser(set2_file)
        
        # Determine what we need to extract (optimization)
        needs_atoms = row.get('atom_identical') == 'N'
        needs_bonds = row.get('bond_identical') == 'N'
        needs_descriptors = row.get('descriptor_identical') == 'N'
        
        # Extract values only for what we need
        values1 = extract_values(parser1, correlation_fields, needs_atoms, needs_bonds, needs_descriptors)
        values2 = extract_values(parser2, correlation_fields, needs_atoms, needs_bonds, needs_descriptors)
        
        # Add values to detailed_row only when different
        for set1_field, set2_field in correlation_fields:
            # Handle single fields
            if set1_field == '_chem_comp.name':
                if row.get('name_identical') == 'N':
                    val1 = format_value(get_field_value(parser1, set1_field))
                    val2 = format_value(get_field_value(parser2, set2_field))
                    detailed_row[f'set1_{set1_field}'] = val1
                    detailed_row[f'set2_{set2_field}'] = val2
            
            elif set1_field == '_chem_comp.type':
                if row.get('type_identical') == 'N':
                    val1 = format_value(get_field_value(parser1, set1_field))
                    val2 = format_value(get_field_value(parser2, set2_field))
                    detailed_row[f'set1_{set1_field}'] = val1
                    detailed_row[f'set2_{set2_field}'] = val2
            
            # Handle grouped fields
            elif set1_field.startswith('_chem_comp_atom.'):
                if needs_atoms:
                    atoms1 = values1.get('_chem_comp_atom_set', [])
                    atoms2 = values2.get('_chem_comp_atom_set', [])
                    diff1, diff2 = format_atom_differences(atoms1, atoms2)
                    detailed_row['set1_atoms'] = diff1
                    detailed_row['set2_atoms'] = diff2
            
            elif set1_field.startswith('_chem_comp_bond.'):
                if needs_bonds:
                    bonds1 = values1.get('_chem_comp_bond_set', [])
                    bonds2 = values2.get('_chem_comp_bond_set', [])
                    diff1, diff2 = format_bond_differences(bonds1, bonds2)
                    detailed_row['set1_bonds'] = diff1
                    detailed_row['set2_bonds'] = diff2
            
            elif set1_field.startswith('_pdbx_chem_comp_descriptor.'):
                if needs_descriptors:
                    desc1 = values1.get('_pdbx_chem_comp_descriptor_set', [])
                    desc2 = values2.get('_pdbx_chem_comp_descriptor_set', [])
                    diff1, diff2 = format_descriptor_differences(desc1, desc2)
                    detailed_row['set1_descriptors'] = diff1
                    detailed_row['set2_descriptors'] = diff2
    
    except Exception as e:
        # In parallel mode, we can't easily log to main process
        # But we should at least try to process what we can
        # Re-raise for now to see what's happening, or log to a file
        # For now, just return the row as-is (will have empty details)
        pass
    
    return detailed_row


def load_correlation_table(csv_path: str) -> List[Tuple[str, str]]:
    """Load correlation table and return list of (set1_field, set2_field) tuples."""
    correlations = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            set1_field = row['wwpdbccd'].strip()
            set2_field = row['ccp4monomerlibrary'].strip()
            if set1_field and set2_field:
                correlations.append((set1_field, set2_field))
    return correlations


def load_checkpoint(checkpoint_file: str) -> Dict[str, Any]:
    """Load checkpoint data if it exists."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_checkpoint(checkpoint_file: str, processed_indices: set, detailed_rows: List[Dict]):
    """Save checkpoint data."""
    try:
        checkpoint_data = {
            'processed_indices': sorted(list(processed_indices)),
            'total_processed': len(processed_indices)
        }
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        if HAS_TQDM:
            tqdm.write(f"Warning: Could not save checkpoint: {e}")
        else:
            print(f"Warning: Could not save checkpoint: {e}")


def main():
    if len(sys.argv) < 4:
        print("Usage: python create_detailed_comparison.py <comparison_csv> <correlation_table> <set1_dir> <set2_dir> [output_csv] [--resume]")
        print("\nExample:")
        print("  python create_detailed_comparison.py comparison_results_20260107_215221.csv \\")
        print("    wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \\")
        print("    C:\\Users\\dharrus\\Documents\\Deborah-Portable\\set1_files \\")
        print("    C:\\Users\\dharrus\\Documents\\Deborah-Portable\\set2_files")
        print("\nOptions:")
        print("  --resume    Resume from checkpoint if available")
        sys.exit(1)
    
    comparison_csv = sys.argv[1]
    correlation_table = sys.argv[2]
    set1_dir = sys.argv[3]
    set2_dir = sys.argv[4]
    output_csv = sys.argv[5] if len(sys.argv) > 5 and not sys.argv[5].startswith('--') else comparison_csv.replace('.csv', '_detailed.csv')
    resume_mode = '--resume' in sys.argv
    
    # Setup checkpoint and cache file names
    checkpoint_file = output_csv.replace('.csv', '_checkpoint.json')
    cache_file = os.path.join(os.path.dirname(output_csv) or '.', 'file_path_cache.json')
    
    # Load correlation table
    print(f"Loading correlation table from {correlation_table}...")
    correlation_fields = load_correlation_table(correlation_table)
    
    # Build file path cache (this is fast if cache file exists)
    build_file_path_cache(set1_dir, set2_dir, cache_file)
    
    # Ensure cache is loaded (it should be in global _file_path_cache)
    # Make a copy for worker processes (multiprocessing needs picklable data)
    file_path_cache_for_workers = dict(_file_path_cache)
    print(f"File path cache ready: {len(file_path_cache_for_workers)} entries")
    
    # Warn if cache seems incomplete (less than expected entries)
    # Typical CCD has 30k+ entries, but this varies
    if len(file_path_cache_for_workers) < 1000:
        print(f"Warning: Cache seems small ({len(file_path_cache_for_workers)} entries).")
        print("  If files aren't being found, try deleting the cache file to rebuild it.")
    
    # Read comparison results
    print(f"Reading comparison results from {comparison_csv}...")
    rows = []
    with open(comparison_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)
    
    total_rows = len(rows)
    rows_with_diffs = sum(1 for r in rows if (
        r.get('name_identical', 'Y') == 'N' or
        r.get('type_identical', 'Y') == 'N' or
        r.get('atom_identical', 'Y') == 'N' or
        r.get('bond_identical', 'Y') == 'N' or
        r.get('descriptor_identical', 'Y') == 'N'
    ))
    print(f"Found {total_rows} total rows ({rows_with_diffs} with differences, {total_rows - rows_with_diffs} identical).")
    
    # Load checkpoint if resuming
    processed_indices = set()
    detailed_rows = []
    start_index = 0
    
    if resume_mode and os.path.exists(checkpoint_file):
        checkpoint = load_checkpoint(checkpoint_file)
        processed_indices = set(checkpoint.get('processed_indices', []))
        start_index = len(processed_indices)
        print(f"Resuming from checkpoint: {start_index} rows already processed")
        
        # Load existing output if it exists to preserve processed rows
        if os.path.exists(output_csv):
            try:
                with open(output_csv, 'r', encoding='utf-8') as f:
                    existing_reader = csv.DictReader(f)
                    for existing_row in existing_reader:
                        detailed_rows.append(existing_row)
                print(f"  Loaded {len(detailed_rows)} rows from existing output file")
            except Exception as e:
                print(f"  Warning: Could not load existing output: {e}")
    
    print("Processing differences...")
    
    # Determine number of workers (use all cores, but cap at 8 for I/O bound tasks)
    num_workers = min(cpu_count(), 8)
    print(f"Using {num_workers} parallel workers")
    
    # Prepare rows for processing (skip already processed, but maintain order)
    rows_to_process = []
    row_indices = []
    for idx, row in enumerate(rows):
        if idx not in processed_indices:
            rows_to_process.append((idx, row))
            row_indices.append(idx)
    
    # Prepare arguments for parallel processing
    # Share the file path cache with workers (use the copy we made)
    process_args = [
        (row, set1_dir, set2_dir, correlation_fields, file_path_cache_for_workers)
        for idx, row in rows_to_process
    ]
    
    # Process rows in parallel
    checkpoint_interval = 100
    processed_count = 0
    
    # Create a dictionary to store results by index to maintain order
    result_dict = {}
    
    with Pool(processes=num_workers) as pool:
        # Use imap for progress tracking
        results = pool.imap(process_row, process_args)
        
        with tqdm(total=len(rows_to_process), initial=0, desc="Processing rows", unit="row", disable=not HAS_TQDM) as pbar:
            for (idx, _), result_row in zip(rows_to_process, results):
                result_dict[idx] = result_row
                processed_indices.add(idx)
                processed_count += 1
                pbar.update(1)
                
                # Save checkpoint periodically
                if processed_count % checkpoint_interval == 0:
                    # Rebuild detailed_rows in correct order
                    temp_detailed = []
                    for i, row in enumerate(rows):
                        if i in processed_indices:
                            if i < len(detailed_rows):
                                temp_detailed.append(detailed_rows[i])
                            elif i in result_dict:
                                temp_detailed.append(result_dict[i])
                            else:
                                temp_detailed.append(row.copy())
                        else:
                            temp_detailed.append(row.copy())
                    
                    save_checkpoint(checkpoint_file, processed_indices, temp_detailed)
                    # Also write partial output
                    try:
                        all_fieldnames = list(fieldnames)
                        value_columns = [
                            'set1__chem_comp.name', 'set2__chem_comp.name',
                            'set1__chem_comp.type', 'set2__chem_comp.group',
                            'set1_atoms', 'set2_atoms',
                            'set1_bonds', 'set2_bonds',
                            'set1_descriptors', 'set2_descriptors',
                        ]
                        for col in value_columns:
                            if col not in all_fieldnames:
                                all_fieldnames.append(col)
                        
                        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=all_fieldnames, extrasaction='ignore')
                            writer.writeheader()
                            writer.writerows(temp_detailed)
                    except Exception as e:
                        if HAS_TQDM:
                            tqdm.write(f"Warning: Could not write partial output: {e}")
                        else:
                            print(f"Warning: Could not write partial output: {e}")
                
                # Update description
                if HAS_TQDM and processed_count % 50 == 0:
                    pbar.set_postfix({
                        'processed': processed_count,
                        'remaining': len(rows_to_process) - processed_count
                    })
    
    # Rebuild detailed_rows in correct order
    for i, row in enumerate(rows):
        if i < len(detailed_rows):
            # Already in detailed_rows (from resume or initial)
            continue
        elif i in result_dict:
            detailed_rows.append(result_dict[i])
        else:
            detailed_rows.append(row.copy())
    
    # Determine all possible fieldnames
    all_fieldnames = list(fieldnames)
    value_columns = [
        'set1__chem_comp.name', 'set2__chem_comp.name',
        'set1__chem_comp.type', 'set2__chem_comp.group',
        'set1_atoms', 'set2_atoms',
        'set1_bonds', 'set2_bonds',
        'set1_descriptors', 'set2_descriptors',
    ]
    
    # Add value columns that appear in any row
    for col in value_columns:
        if col not in all_fieldnames:
            all_fieldnames.append(col)
    
    # Write final output
    print(f"Writing detailed comparison to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(detailed_rows)
    
    # Clean up checkpoint file on successful completion
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            print(f"Checkpoint file removed (processing complete)")
        except Exception:
            pass
    
    print(f"Done! Created {output_csv} with {len(detailed_rows)} rows.")
    print(f"Performance stats: {len(_parsed_file_cache)} CIF files cached in memory")


if __name__ == '__main__':
    main()

