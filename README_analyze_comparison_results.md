# Analysis Script for Comparison Results

## Overview

`analyze_comparison_results.py` is a Python script that analyzes CSV files containing comparison results between two data sources:
- **Set 1 (wwPDB CCD)**: wwPDB Chemical Component Dictionary
- **Set 2 (CCP4 Monomer Library)**: CCP4 Monomer Library

It generates comprehensive statistics and identifies outdated CCP4 Monomer Library files based on modification dates.

## Features

The script provides:

- **Summary Statistics**: Total entry count and overall identity statistics
- **Field-by-Field Analysis**: Breakdown of identity matches for name, type, atom, bond, and descriptor fields
- **Date Comparison**: Identifies when CCP4 Monomer Library files are outdated (when `ccp4_modified_date` is before `wwpdb_modified_date`)
- **Outdated Files Report**: Detailed list of outdated entries with days behind statistics
- **Automatic Report Generation**: Automatically generates timestamped output files

## Requirements

- Python 3.6 or higher
- Standard library only (no external dependencies required)

## Input File Format

The script works with both:
- **Regular comparison CSV files** (from `ccd_sync.py`)
- **Detailed comparison CSV files** (from `create_detailed_comparison.py`)

Both formats include the same standard columns needed for analysis. The detailed CSV files have additional columns with actual values, but these are ignored by the analysis script.

The script expects a CSV file with the following columns:

- `ccd_code`: The CCD code identifier (filename without extension, e.g., "000", "A1A15")
- `name_identical`: Y/N - Whether `_chem_comp.name` matches between Set 1 (wwPDB CCD) and Set 2 (CCP4 Monomer Library)
- `type_identical`: Y/N - Whether `_chem_comp.type` (Set 1) matches `_chem_comp.group` (Set 2)
- `atom_identical`: Y/N - Whether all atoms match as sets. Each atom is compared by:
  - `atom_id` (Set 1 - wwPDB CCD) vs `atom_id` (Set 2 - CCP4 Monomer Library)
  - `type_symbol` (Set 1) vs `type_symbol` (Set 2)
  - `charge` (Set 1) vs `charge` (Set 2)
- `bond_identical`: Y/N - Whether all bonds match as sets. Each bond is compared by:
  - `atom_id_1` (Set 1 - wwPDB CCD) vs `atom_id_1` (Set 2 - CCP4 Monomer Library)
  - `atom_id_2` (Set 1) vs `atom_id_2` (Set 2)
  - `value_order` (Set 1) vs `type` (Set 2) - with mapping: SING↔SINGLE, DOUB↔DOUBLE
  - `pdbx_aromatic_flag` (Set 1) vs `aromatic` (Set 2)
- `descriptor_identical`: Y/N - Whether all descriptors match as sets. Each descriptor is compared by:
  - `type` (Set 1 - wwPDB CCD) vs `type` (Set 2 - CCP4 Monomer Library)
  - `program` (Set 1) vs `program` (Set 2)
  - `program_version` (Set 1) vs `program_version` (Set 2)
  - `descriptor` (Set 1) vs `descriptor` (Set 2)
- `overall_identical`: Y/N - Whether all fields (name, type, atom, bond, descriptor) are identical
- `wwpdb_modified_date`: Date in YYYY-MM-DD format - Last modification date from Set 1 (wwPDB CCD)
- `ccp4_modified_date`: Date in YYYY-MM-DD format - Last commit date from Set 2 (CCP4 Monomer Library)

## Usage

### Basic Usage

```bash
python analyze_comparison_results.py <input_file.csv>
```

The script will automatically generate an output file with a timestamp.

### Custom Output File

```bash
python analyze_comparison_results.py <input_file.csv> -o <output_file.txt>
```

### Examples

```bash
# Analyze a comparison results file (auto-generates output)
python analyze_comparison_results.py comparison_results_20260107_215221.csv

# Specify a custom output filename
python analyze_comparison_results.py comparison_results.csv -o my_report.txt
```

## Output Format

The generated report includes:

### 1. Summary Statistics
- Total number of entries analyzed

### 2. Overall Identity
- Count and percentage of entries that are identical vs. different

### 3. Field-by-Field Identity
- For each field (Name, Type, Atom, Bond, Descriptor):
  - Count and percentage of identical entries
  - Count and percentage of different entries

### 4. Date Comparison
- **CCP4 Outdated**: Count of entries where `ccp4_modified_date < wwpdb_modified_date` (CCP4 Monomer Library is older than wwPDB CCD)
- **CCP4 Up-to-Date**: Count of entries where `ccp4_modified_date > wwpdb_modified_date` (CCP4 Monomer Library is newer than wwPDB CCD)
- **Dates Equal**: Count of entries where dates are the same
- **Missing Dates**: Count of entries with missing or invalid dates

### 5. Outdated CCP4 Monomer Library Files Report
- Total count of outdated entries (where CCP4 Monomer Library date is before wwPDB CCD date)
- Top 20 most outdated entries showing:
  - CCD Code
  - wwPDB CCD Date
  - CCP4 Monomer Library Date
  - Days Behind
  - Overall Identical status
- Statistics on outdated entries:
  - Average days behind
  - Maximum days behind
  - Minimum days behind
- Breakdown by identity status:
  - Outdated and different
  - Outdated but identical

## Output File Naming

When no output file is specified, the script automatically generates a filename using the format:

```
analysis_report_<input_filename_stem>_YYYYMMDD_HHMMSS.txt
```

For example:
- Input: `comparison_results_20260107_215221.csv`
- Output: `analysis_report_comparison_results_20260107_215221_20260107_223033.txt`

The timestamp format (YYYYMMDD_HHMMSS) ensures files are easily sortable and filesystem-safe.

## Interpreting Results

### Outdated Files

An entry is considered **outdated** when:
- `ccp4_modified_date < wwpdb_modified_date`

This indicates that the CCP4 Monomer Library file is older than the wwPDB CCD version and may need to be updated.

### Identity Fields

Each identity field (name, type, atom, bond, descriptor) can have values:
- **Y**: Fields match between sources
- **N**: Fields differ between sources

**Important Notes:**
- **Name and Type**: Single field comparisons - values must match exactly (after normalization)
- **Atom, Bond, and Descriptor**: Set-based comparisons - the complete sets of atoms/bonds/descriptors must match between both sources. The order doesn't matter, but all entries must be present in both sets with identical values.
- **Overall Identical**: Only Y if ALL fields (name, type, atom, bond, descriptor) are Y. If any field is N, overall_identical is N.

## Error Handling

The script handles:
- Missing input files (exits with error message)
- Invalid date formats (counted as missing dates)
- File write errors (displays warning but continues)

## Notes

- The script prints progress messages to stderr, so you can redirect stdout if needed
- All dates are expected in YYYY-MM-DD format
- Large CSV files are processed efficiently using Python's csv module

