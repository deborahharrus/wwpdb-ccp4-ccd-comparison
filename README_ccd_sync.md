# CCD Sync - mmCIF File Comparison Tool

## Overview

`ccd_sync.py` is a Python script that compares mmCIF files from two different sources:
- **Set 1 (wwPDB CCD)**: WWPDB Chemical Component Dictionary files from `https://files.wwpdb.org/pub/pdb/data/monomers/components.cif.gz` (downloaded and split)
- **Set 2 (CCP4 Monomer Library)**: CCP4 Monomer Library files from GitHub repository at `https://github.com/MonomerLibrary/monomers/tree/master/`

The script performs detailed comparisons of chemical component definitions (CCD) files and generates reports on differences between the two data sources.

## Features

- **Multiple Operation Modes**: Local file comparison, download mode, online comparison, and date refetching
- **Comprehensive Comparison**: Compares name, type, atoms, bonds, and descriptors between files
- **Date Tracking**: Retrieves and compares modification dates from both sources
- **Batch Processing**: Efficiently processes large numbers of files
- **Missing File Detection**: Identifies files present in one source but not the other
- **GitHub API Integration**: Uses GitHub API to retrieve commit dates (with optional token for higher rate limits)
- **Progress Tracking**: Optional progress bars with `tqdm` library

## Requirements

- Python 3.6 or higher
- Standard library modules (no external dependencies required)
- **Optional**: `tqdm` for progress bars
  ```bash
  pip install tqdm
  ```

## Data Sources

### Set 1 (wwPDB CCD)
- **Name**: wwPDB Chemical Component Dictionary
- Source: `https://files.wwpdb.org/pub/pdb/data/monomers/components.cif.gz`
- Format: Single gzipped archive containing all CCD files
- File structure: Files are split and organized by CCD code (e.g., `0/000/000.cif` for 3-character codes, `5/A1A15/A1A15.cif` for 5-character codes)
- Date source: `_chem_comp.pdbx_modified_date` field in mmCIF files

### Set 2 (CCP4 Monomer Library)
- **Name**: CCP4 Monomer Library
- Source: `https://github.com/MonomerLibrary/monomers/tree/master/`
- Format: Individual files in GitHub repository
- File structure: Flat structure (e.g., `0/000.cif`)
- Date source: Last commit date from GitHub API

## Correlation Table

The script requires a correlation table CSV file that maps fields between Set 1 and Set 2. This table defines:
- Which fields to compare
- How fields map between the two sources (field names may differ)
- Grouped comparisons (e.g., comparing triplets of fields together)

The correlation table format should have columns mapping Set 1 fields to Set 2 fields. Grouped items are indicated with slashes (e.g., `item1/item2/item3`).

## Usage

### Basic Syntax

```bash
python ccd_sync.py [OPTIONS]
```

### Operation Modes

#### 1. Local Mode (`--mode local`)
Compare files that are already downloaded locally.

```bash
python ccd_sync.py --mode local --correlation-table <correlation_table.csv>
```

**Requirements:**
- Files must be in `set1_files/` and `set2_files/` directories (or custom directories specified with `--set1-dir` and `--set2-dir`)
- Correlation table CSV file

#### 2. Download Mode (`--mode download`)
Download files from both sources and optionally compare them.

```bash
# Download both sets and compare
python ccd_sync.py --mode download --download-set1 --download-set2 --correlation-table <correlation_table.csv>

# Download only Set 1
python ccd_sync.py --mode download --download-set1 --correlation-table <correlation_table.csv>

# Download only Set 2
python ccd_sync.py --mode download --download-set2 --correlation-table <correlation_table.csv>

# Download without comparing (download-only mode)
python ccd_sync.py --mode download --download-set1 --download-set2 --download-only
```

**Note:** In `--download-only` mode, the `--correlation-table` option is not required.

#### 3. Online Mode (`--mode online`)
Compare files directly from remote sources without downloading.

```bash
# Compare all files
python ccd_sync.py --mode online --correlation-table <correlation_table.csv>

# Compare specific CCD codes
python ccd_sync.py --mode online --ccd-codes "000,001,A1A15" --correlation-table <correlation_table.csv>
```

#### 4. Refetch Dates Mode (`--mode refetch-dates`)
Re-fetch Set 2 (GitHub) commit dates from a previous output CSV file.

```bash
python ccd_sync.py --mode refetch-dates --input-csv <previous_output.csv>
```

This mode is useful when:
- GitHub API rate limits were hit during initial run
- You want to update dates for entries that had missing dates
- You want to refresh dates for entries that had placeholder dates (today's date)

## Command-Line Options

### Required Options

- `--correlation-table <file>`: Path to correlation table CSV file (required for comparison modes, not needed for `--download-only`)

### Mode Options

- `--mode {local,download,online,refetch-dates}`: Operation mode (default: `local`)

### Download Options

- `--download-set1`: Download Set 1 files from WWPDB archive
- `--download-set2`: Download Set 2 files from GitHub
- `--download-only`: Only download files without comparing (use with `--mode download`)

### Directory Options

- `--set1-dir <dir>`: Directory for Set 1 files (default: `set1_files`)
- `--set2-dir <dir>`: Directory for Set 2 files (default: `set2_files`)

### Output Options

- `--output <file>`: Output CSV filename (default: `comparison_results.csv`)
  - Note: Timestamp is automatically appended to the filename

### Filtering Options

- `--ccd-codes <codes>`: Comma-separated list of CCD codes to compare (e.g., `"000,001,A1A15"`)
  - Works with `--mode online`
- `--limit <number>`: Limit the number of file pairs to compare (useful for testing)

### GitHub Options

- `--github-token <token>`: GitHub personal access token for higher API rate limits
  - Get a token at: https://github.com/settings/tokens
  - Alternatively, create a file named `github_token.txt` in the script directory

### Refetch Mode Options

- `--input-csv <file>`: Input CSV file for `--mode refetch-dates` (required for refetch mode)

## Output Files

### Main Output CSV

The script generates a timestamped CSV file with the following columns:

- `ccd_code`: CCD code identifier (filename without extension)
- `name_identical`: Y/N - Whether `_chem_comp.name` matches between Set 1 (wwPDB CCD) and Set 2 (CCP4 Monomer Library)
- `type_identical`: Y/N - Whether `_chem_comp.type` (Set 1) matches `_chem_comp.group` (Set 2)
- `atom_identical`: Y/N - Whether atom data matches (atom_id, type_symbol, charge)
- `bond_identical`: Y/N - Whether bond data matches (atom_id_1, atom_id_2, order/type, aromatic flag)
- `descriptor_identical`: Y/N - Whether descriptor data matches
- `overall_identical`: Y/N - Whether all fields match
- `wwpdb_modified_date`: Modification date from Set 1 (wwPDB CCD)
- `ccp4_modified_date`: Last commit date from Set 2 (CCP4 Monomer Library)

**Filename format:** `<output_name>_YYYYMMDD_HHMMSS.csv`

### Missing Files Report

If files are missing from one or both sources, an additional CSV file is generated:

- `*_missing_files.csv`: Lists CCD codes that are missing from Set 1, Set 2, or both

**Columns:**
- `ccd_code`: CCD code identifier
- `missing_from_set1`: Y/N
- `missing_from_set2`: Y/N
- `missing_from_both`: Y/N

## Comparison Details

### Fields Compared

The script compares specific mmCIF fields between Set 1 (WWPDB) and Set 2 (GitHub/MonomerLibrary). The exact fields are:

#### 1. Name (`name_identical`)
- **Set 1 (wwPDB CCD)**: `_chem_comp.name`
- **Set 2 (CCP4 Monomer Library)**: `_chem_comp.name`
- **Comparison**: Direct single-field comparison
- **Result**: Y if values match exactly (after normalization), N otherwise

#### 2. Type (`type_identical`)
- **Set 1 (wwPDB CCD)**: `_chem_comp.type`
- **Set 2 (CCP4 Monomer Library)**: `_chem_comp.group`
- **Comparison**: Direct single-field comparison
- **Result**: Y if values match exactly (after normalization), N otherwise

#### 3. Atoms (`atom_identical`)
- **Comparison Type**: Grouped comparison (all atoms must match as sets)
- **Fields compared for each atom**:
  - **Set 1 (wwPDB CCD)**: `_chem_comp_atom.atom_id`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_atom.atom_id`
  
  - **Set 1 (wwPDB CCD)**: `_chem_comp_atom.type_symbol`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_atom.type_symbol`
  
  - **Set 1 (wwPDB CCD)**: `_chem_comp_atom.charge`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_atom.charge`
- **How it works**: Each atom is represented as a tuple of (atom_id, type_symbol, charge). The script compares the complete set of atoms from Set 1 (wwPDB CCD) with the complete set from Set 2 (CCP4 Monomer Library). Both sets must contain the same atoms with identical values.
- **Result**: Y if both sets contain identical atoms (same atom_id, type_symbol, and charge for all atoms), N otherwise

#### 4. Bonds (`bond_identical`)
- **Comparison Type**: Grouped comparison (all bonds must match as sets)
- **Fields compared for each bond**:
  - **Set 1 (wwPDB CCD)**: `_chem_comp_bond.atom_id_1`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_bond.atom_id_1`
  
  - **Set 1 (wwPDB CCD)**: `_chem_comp_bond.atom_id_2`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_bond.atom_id_2`
  
  - **Set 1 (wwPDB CCD)**: `_chem_comp_bond.value_order`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_bond.type`
  - **Special mapping**: 
    - `SING` (Set 1) ↔ `SINGLE` (Set 2)
    - `DOUB` (Set 1) ↔ `DOUBLE` (Set 2)
  
  - **Set 1 (wwPDB CCD)**: `_chem_comp_bond.pdbx_aromatic_flag`
  - **Set 2 (CCP4 Monomer Library)**: `_chem_comp_bond.aromatic`
- **How it works**: Each bond is represented as a tuple of (atom_id_1, atom_id_2, order/type, aromatic_flag). The script compares the complete set of bonds from Set 1 (wwPDB CCD) with the complete set from Set 2 (CCP4 Monomer Library). Both sets must contain the same bonds with identical values.
- **Result**: Y if both sets contain identical bonds (same atom pairs, order/type, and aromatic flags), N otherwise

#### 5. Descriptors (`descriptor_identical`)
- **Comparison Type**: Grouped comparison (all descriptors must match as sets)
- **Fields compared for each descriptor**:
  - **Set 1 (wwPDB CCD)**: `_pdbx_chem_comp_descriptor.type`
  - **Set 2 (CCP4 Monomer Library)**: `_pdbx_chem_comp_descriptor.type`
  
  - **Set 1 (wwPDB CCD)**: `_pdbx_chem_comp_descriptor.program`
  - **Set 2 (CCP4 Monomer Library)**: `_pdbx_chem_comp_descriptor.program`
  
  - **Set 1 (wwPDB CCD)**: `_pdbx_chem_comp_descriptor.program_version`
  - **Set 2 (CCP4 Monomer Library)**: `_pdbx_chem_comp_descriptor.program_version`
  
  - **Set 1 (wwPDB CCD)**: `_pdbx_chem_comp_descriptor.descriptor`
  - **Set 2 (CCP4 Monomer Library)**: `_pdbx_chem_comp_descriptor.descriptor`
- **How it works**: Each descriptor is represented as a tuple of (type, program, program_version, descriptor). The script compares the complete set of descriptors from Set 1 (wwPDB CCD) with the complete set from Set 2 (CCP4 Monomer Library). Both sets must contain the same descriptors with identical values.
- **Result**: Y if both sets contain identical descriptors (same type, program, program_version, and descriptor text), N otherwise

### Normalization Rules

The script applies several normalization rules during comparison:

- **Case-insensitive comparison**: Values are converted to lowercase
- **Quote removal**: Quotes are removed from descriptor values
- **Multi-line value handling**: Newlines in multi-line values (like names) are removed before comparison (they're formatting artifacts, not content)
- **Bond order mapping**: 
  - `SING` (Set 1) ↔ `SINGLE` (Set 2)
  - `DOUB` (Set 1) ↔ `DOUBLE` (Set 2)
- **Bond atom ordering normalization**: Bonds are treated as undirected, so "C-OXT" and "OXT-C" are considered the same bond. The script normalizes atom ordering before comparison to ensure only true differences are reported.

## Examples

### Example 1: Compare Local Files

```bash
python ccd_sync.py --mode local --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

### Example 2: Download and Compare All Files

```bash
python ccd_sync.py --mode download --download-set1 --download-set2 --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

### Example 3: Compare Specific Codes Online

```bash
python ccd_sync.py --mode online --ccd-codes "000,001,A1A15" --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

### Example 4: Test with Limited Files

```bash
python ccd_sync.py --mode local --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv --limit 1000
```

### Example 5: Refetch Missing Dates

```bash
python ccd_sync.py --mode refetch-dates --input-csv comparison_results_20260107_215221.csv
```

### Example 6: Use GitHub Token

```bash
python ccd_sync.py --mode online --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv --github-token YOUR_TOKEN_HERE
```

Or create `github_token.txt` in the script directory with your token.

### Example 7: Use Custom Directory Paths

If your Set1 and Set2 folders are located elsewhere on your system:

```bash
# Compare files from custom directories
python ccd_sync.py --mode local --set1-dir "C:\Path\To\Set1" --set2-dir "C:\Path\To\Set2" --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv

# Download to custom directories
python ccd_sync.py --mode download --download-set1 --download-set2 --set1-dir "C:\Path\To\Set1" --set2-dir "C:\Path\To\Set2" --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

## GitHub API Rate Limits

The script uses the GitHub API to retrieve commit dates for Set 2 files. Without a token:
- Rate limit: 60 requests/hour (unauthenticated)
- The script will warn you if rate limits are exceeded

With a GitHub personal access token:
- Rate limit: 5,000 requests/hour (authenticated)
- Significantly faster processing for large datasets

## File Structure

After downloading, the directory structure will be:

```
set1_files/
  ├── 0/
  │   └── 000/
  │       └── 000.cif
  ├── 5/
  │   └── A1A15/
  │       └── A1A15.cif
  └── ...

set2_files/
  ├── 0/
  │   └── 000.cif
  ├── 5/
  │   └── A1A15.cif
  └── ...
```

## Error Handling

The script handles:
- Missing files gracefully (reported in missing files CSV)
- Network errors (retries and continues)
- GitHub API rate limits (warns and continues with available data)
- Invalid file formats (logs error and continues)
- Missing correlation table entries (skips those comparisons)

## Performance Tips

1. **Use GitHub Token**: Significantly speeds up date retrieval for large datasets
2. **Download First**: Use `--download-only` mode first, then use `--mode local` for faster subsequent comparisons
3. **Limit for Testing**: Use `--limit` option to test with a subset of files
4. **Parallel Processing**: The script uses multiprocessing for efficient batch operations

## Notes

- The script automatically appends timestamps to output filenames to prevent overwriting
- Progress bars (if `tqdm` is installed) show download and processing progress
- The script supports resuming downloads if files already exist
- Date comparisons help identify when CCP4 Monomer Library files may be outdated compared to wwPDB CCD sources

## Related Scripts

- `create_detailed_comparison.py`: Creates an enhanced comparison CSV with actual values for differences
  - Takes the output from `ccd_sync.py` and adds detailed columns showing what actually differs
  - Includes file path caching and parallel processing for performance
  - See `README_comparison_differences.md` for details on the output format
- `analyze_comparison_results.py`: Analyzes the output CSV files and generates statistics reports
  - See `README_analyze_comparison_results.md` for details

