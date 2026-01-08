# wwpdb-ccp4-ccd-comparison
Python toolkit for comparing Chemical Component Dictionary (CCD) files between the wwPDB CCD and CCP4 Monomer Library. Identifies differences in names, types, atoms, bonds, and descriptors, tracks modification dates, and generates detailed comparison reports. The long term goal is to track updates, and maintain consistency between these two important structural biology data sources.

**Author**: Deborah Harrus

## Overview

This toolkit provides tools to:

- **Compare** CCD files from two sources (wwPDB CCD and CCP4 Monomer Library)
- **Identify differences** in names, types, atoms, bonds, and descriptors
- **Track modification dates** to identify outdated entries
- **Generate detailed reports** showing exactly what differs between sources
- **Analyze statistics** on differences and data quality

## Quick Start

### 1. Install Requirements

```bash
# Optional: Install tqdm for progress bars
pip install tqdm
```

### 2. Download and Compare Files

```bash
# Download files from both sources and compare
python ccd_sync.py --mode download --download-set1 --download-set2 \
  --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

### 3. Generate Detailed Comparison

```bash
# Create detailed CSV with actual difference values
python create_detailed_comparison.py \
  comparison_results_YYYYMMDD_HHMMSS.csv \
  wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \
  set1_files set2_files
```

### 4. Analyze Results

```bash
# Generate statistics report
python analyze_comparison_results.py comparison_results_YYYYMMDD_HHMMSS_detailed.csv
```

## Project Structure

```
ccd_sync/
├── ccd_sync.py                          # Main comparison script
├── create_detailed_comparison.py        # Enhanced comparison with actual values
├── analyze_comparison_results.py        # Statistics and analysis
├── find_and_copy_cif.py                 # Utility to find and copy CIF files
├── README.md                            # This file (project overview)
├── README_ccd_sync.md                   # Detailed documentation for ccd_sync.py
├── README_comparison_differences.md     # Guide to understanding differences
├── README_analyze_comparison_results.md # Documentation for analysis script
└── wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv  # Field mapping table
```

## Workflow

### Typical Workflow

```
1. Download Files
   └─> ccd_sync.py --mode download --download-set1 --download-set2
   
2. Compare Files
   └─> ccd_sync.py --mode local
       └─> Generates: comparison_results_YYYYMMDD_HHMMSS.csv
       
3. Create Detailed Comparison
   └─> create_detailed_comparison.py
       └─> Generates: comparison_results_YYYYMMDD_HHMMSS_detailed.csv
       
4. Analyze Results
   └─> analyze_comparison_results.py
       └─> Generates: analysis_report_YYYYMMDD_HHMMSS.txt
```

### Workflow Diagram

```
┌─────────────────┐
│  Download Mode  │  Download files from wwPDB and CCP4
│  (Optional)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Compare Mode   │  Compare all CCD files
│  (ccd_sync.py)  │  └─> Output: comparison_results.csv
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Detailed Comparison    │  Add actual difference values
│  (create_detailed_      │  └─> Output: *_detailed.csv
│   comparison.py)        │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Analysis               │  Generate statistics
│  (analyze_comparison_   │  └─> Output: analysis_report.txt
│   results.py)           │
└─────────────────────────┘
```

## Scripts Overview

### 1. `ccd_sync.py` - Main Comparison Tool

The primary script for comparing CCD files between two sources.

**Key Features:**
- Multiple operation modes (local, download, online, refetch-dates)
- Compares: names, types, atoms, bonds, descriptors
- Tracks modification dates
- Handles missing files gracefully
- Supports GitHub API for faster date retrieval

**Quick Example:**
```bash
# Compare local files
python ccd_sync.py --mode local \
  --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

📖 **See [README_ccd_sync.md](README_ccd_sync.md) for complete documentation**

### 2. `create_detailed_comparison.py` - Enhanced Comparison

Creates a detailed CSV showing actual values for differences (not just Y/N flags).

**Key Features:**
- Shows actual differing values (names, atoms, bonds, etc.)
- File path caching for fast lookups
- Parallel processing for performance (4-8x speedup)
- Resume capability if interrupted
- Only extracts data types that differ (optimization)

**Quick Example:**
```bash
python create_detailed_comparison.py \
  comparison_results_20260108_141604.csv \
  wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \
  set1_files set2_files
```

**Performance:**
- First run: Builds file path cache (~few minutes)
- Subsequent runs: Uses cache (instant file lookup)
- Parallel processing: 12-24 rows/second (vs 3 rows/second sequential)

### 3. `analyze_comparison_results.py` - Statistics and Analysis

Analyzes comparison results and generates comprehensive statistics reports.

**Key Features:**
- Summary statistics on identity matches
- Field-by-field breakdown
- Date comparison analysis
- Outdated file identification
- Automatic report generation

**Quick Example:**
```bash
python analyze_comparison_results.py comparison_results_20260108_141604_detailed.csv
```

📖 **See [README_analyze_comparison_results.md](README_analyze_comparison_results.md) for complete documentation**

### 4. `find_and_copy_cif.py` - Utility Script

Helper script to find and copy specific CCD CIF files for inspection.

**Quick Example:**
```bash
python find_and_copy_cif.py 2J0
```

## Data Sources

### Set 1: wwPDB Chemical Component Dictionary
- **Source**: `https://files.wwpdb.org/pub/pdb/data/monomers/components.cif.gz`
- **Format**: Single gzipped archive (downloaded and split into individual files)
- **File Structure**: `{last_char}/{code}/{code}.cif` (e.g., `0/000/000.cif`)
- **Date Source**: `_chem_comp.pdbx_modified_date` field in mmCIF files

### Set 2: CCP4 Monomer Library
- **Source**: `https://github.com/MonomerLibrary/monomers/tree/master/`
- **Format**: Individual files in GitHub repository
- **File Structure**: `{first_char}/{code}.cif` (e.g., `0/000.cif`)
- **Date Source**: Last commit date from GitHub API

## Output Files

### Comparison Results CSV

Generated by `ccd_sync.py`:
- `comparison_results_YYYYMMDD_HHMMSS.csv`: Main comparison results
- `comparison_results_YYYYMMDD_HHMMSS_missing_files.csv`: Files missing from one or both sources

**Columns:**
- `ccd_code`: CCD identifier
- `name_identical`, `type_identical`, `atom_identical`, `bond_identical`, `descriptor_identical`: Y/N flags
- `overall_identical`: Y if all fields match
- `wwpdb_modified_date`, `ccp4_modified_date`: Modification dates

### Detailed Comparison CSV

Generated by `create_detailed_comparison.py`:
- `comparison_results_YYYYMMDD_HHMMSS_detailed.csv`: Enhanced CSV with actual difference values

**Additional Columns:**
- `set1__chem_comp.name`, `set2__chem_comp.name`: Actual name values when different
- `set1_atoms`, `set2_atoms`: Only differing atoms (formatted as "ATOM_ID(TYPE,CHARGE)")
- `set1_bonds`, `set2_bonds`: Only differing bonds (formatted as "ATOM1-ATOM2(ORDER,AROMATIC)")
- `set1_descriptors`, `set2_descriptors`: Only differing descriptors

### Analysis Report

Generated by `analyze_comparison_results.py`:
- `analysis_report_YYYYMMDD_HHMMSS.txt`: Comprehensive statistics report

**Includes:**
- Summary statistics
- Field-by-field identity breakdown
- Date comparison analysis
- Outdated file identification
- Top 20 most outdated entries

## Understanding Differences

The comparison identifies differences in:

1. **Names**: Chemical component names
2. **Types**: Component classification (e.g., "peptide-like" vs "NON-POLYMER")
3. **Atoms**: Atom definitions (ID, type, charge)
4. **Bonds**: Bond connectivity and properties (order, aromaticity)
5. **Descriptors**: Chemical descriptors (SMILES, InChI, etc.)

**Important Normalizations:**
- Bond atom ordering: "C-OXT" and "OXT-C" are treated as the same
- Bond orders: SING ↔ SINGLE, DOUB ↔ DOUBLE are normalized
- Multi-line values: Newlines are removed (formatting artifacts)
- Case-insensitive: All comparisons are case-insensitive

📖 **See [README_comparison_differences.md](README_comparison_differences.md) for detailed explanation of difference types**

## Requirements

- **Python**: 3.6 or higher
- **Standard Library**: No external dependencies required (except optional `tqdm` for progress bars)
- **Optional**: `tqdm` for progress bars
  ```bash
  pip install tqdm
  ```

## Installation

1. Clone or download this repository
2. Ensure you have Python 3.6+
3. (Optional) Install tqdm: `pip install tqdm`
4. Prepare your correlation table CSV file (see `wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv` for format)

## Usage Examples

### Example 1: Complete Workflow

```bash
# Step 1: Download files (if not already downloaded)
python ccd_sync.py --mode download --download-set1 --download-set2 \
  --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv

# Step 2: Compare files
python ccd_sync.py --mode local \
  --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv

# Step 3: Create detailed comparison
python create_detailed_comparison.py \
  comparison_results_20260108_141604.csv \
  wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \
  set1_files set2_files

# Step 4: Analyze results
python analyze_comparison_results.py \
  comparison_results_20260108_141604_detailed.csv
```

### Example 2: Compare Specific Codes

```bash
# Compare only specific CCD codes online
python ccd_sync.py --mode online \
  --ccd-codes "000,001,2J0" \
  --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
```

### Example 3: Resume Detailed Comparison

If `create_detailed_comparison.py` is interrupted, you can resume:

```bash
python create_detailed_comparison.py \
  comparison_results_20260108_141604.csv \
  wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \
  set1_files set2_files \
  output.csv --resume
```

## Performance Tips

1. **Use GitHub Token**: Significantly speeds up date retrieval for large datasets
   ```bash
   python ccd_sync.py --mode online --github-token YOUR_TOKEN
   ```

2. **Download First**: Use `--download-only` mode first, then `--mode local` for faster subsequent comparisons

3. **File Path Cache**: `create_detailed_comparison.py` automatically caches file paths (first run builds cache, subsequent runs are much faster)

4. **Parallel Processing**: `create_detailed_comparison.py` uses multiple CPU cores automatically (4-8x speedup)

5. **Resume Capability**: Both scripts support resuming if interrupted

## Common Use Cases

### Identify Outdated Files
```bash
# Compare and analyze to find outdated CCP4 files
python ccd_sync.py --mode local --correlation-table wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv
python analyze_comparison_results.py comparison_results_*.csv
# Check the "OUTDATED CCP4 FILES" section in the report
```

### Find Specific Differences
```bash
# Create detailed comparison to see actual values
python create_detailed_comparison.py comparison_results_*.csv \
  wwpd_ccd_to_ccp4_monomer_library_correlation_table.csv \
  set1_files set2_files
# Open the detailed CSV and filter for specific CCD codes
```

### Inspect Specific Files
```bash
# Copy files for a specific CCD code to current directory
python find_and_copy_cif.py 2J0
# Files will be copied as example_set1_2J0.cif and example_set2_2J0.cif
```

## Documentation

- **[README_ccd_sync.md](README_ccd_sync.md)**: Complete documentation for the main comparison script
- **[README_comparison_differences.md](README_comparison_differences.md)**: Guide to understanding different types of differences
- **[README_analyze_comparison_results.md](README_analyze_comparison_results.md)**: Documentation for the analysis script

## Key Features

### Comparison Logic

- **Set-based comparison**: Atoms, bonds, and descriptors are compared as complete sets (order doesn't matter)
- **Normalization**: Handles formatting differences (SING vs SINGLE, case differences, etc.)
- **Bond normalization**: Treats bonds as undirected (C-OXT = OXT-C)
- **Multi-line handling**: Properly handles multi-line values in CIF files

### Performance Optimizations

- **File path caching**: Pre-scans directories once, reuses cache for fast lookups
- **Parallel processing**: Uses multiple CPU cores for faster processing
- **Conditional extraction**: Only extracts data types that differ
- **Resume capability**: Can resume from checkpoints if interrupted

### Error Handling

- **Missing files**: Gracefully handles files missing from one or both sources
- **Network errors**: Retries and continues on network failures
- **GitHub API limits**: Warns and continues with available data
- **Invalid formats**: Logs errors and continues processing

## Statistics

Based on typical comparison runs:

- **Total components**: ~32,000-33,000
- **Completely identical**: ~22-23%
- **Have differences**: ~77-78%
- **Most common differences**: Bonds (67.9%), Atoms (36.0%)
- **Outdated CCP4 files**: ~12-13% (where CCP4 date < wwPDB date)

## Contributing

This is a specialized tool for comparing chemical component dictionaries. If you find issues or have suggestions, please:

1. Check existing documentation first
2. Review the comparison logic in the code
3. Test with a small subset of files first (`--limit` option)

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

## Author

**Deborah Harrus**

## Results

Example comparison results from recent runs are available in dated folders (e.g., `2026-01-08_run/`). These folders contain:
- Comparison results CSV files
- Detailed comparison CSV files
- Analysis reports
- Missing files reports

These can serve as examples of the output format and help understand the types of differences found between the two data sources.

## Acknowledgments

- **wwPDB**: For providing the Chemical Component Dictionary
- **CCP4**: For maintaining the Monomer Library
- **GitHub**: For hosting the CCP4 Monomer Library repository

## Support

For questions or issues:
1. Check the relevant README file for the script you're using
2. Review the examples in this README
3. Check the output CSV files for clues about what's happening

---

**Last Updated**: January 2025

