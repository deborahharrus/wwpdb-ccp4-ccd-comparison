#!/usr/bin/env python3
"""
Analysis script for comparison results CSV files.

This script analyzes comparison results and reports:
- Statistics on identity fields
- Date comparisons to identify outdated CCP4 files
- Summary statistics
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def analyze_comparison_results(csv_file: str) -> Dict:
    """Analyze comparison results CSV file.
    
    Args:
        csv_file: Path to the comparison results CSV file
        
    Returns:
        Dictionary containing analysis results
    """
    results = {
        'total_entries': 0,
        'identity_counts': defaultdict(int),
        'overall_identical': {'Y': 0, 'N': 0},
        'date_comparison': {
            'ccp4_outdated': 0,
            'ccp4_up_to_date': 0,
            'dates_equal': 0,
            'missing_dates': 0
        },
        'outdated_entries': []
    }
    
    identity_fields = [
        'name_identical', 'type_identical', 'atom_identical',
        'bond_identical', 'descriptor_identical', 'overall_identical'
    ]
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                results['total_entries'] += 1
                ccd_code = row.get('ccd_code', '')
                
                # Count identity fields
                for field in identity_fields:
                    value = row.get(field, '').strip().upper()
                    if value in ['Y', 'N']:
                        results['identity_counts'][f'{field}_{value}'] += 1
                
                # Count overall identical
                overall = row.get('overall_identical', '').strip().upper()
                if overall in ['Y', 'N']:
                    results['overall_identical'][overall] += 1
                
                # Compare dates
                wwpdb_date_str = row.get('wwpdb_modified_date', '').strip()
                ccp4_date_str = row.get('ccp4_modified_date', '').strip()
                
                if not wwpdb_date_str or not ccp4_date_str:
                    results['date_comparison']['missing_dates'] += 1
                    continue
                
                wwpdb_date = parse_date(wwpdb_date_str)
                ccp4_date = parse_date(ccp4_date_str)
                
                if wwpdb_date is None or ccp4_date is None:
                    results['date_comparison']['missing_dates'] += 1
                    continue
                
                if ccp4_date < wwpdb_date:
                    # CCP4 file is outdated
                    results['date_comparison']['ccp4_outdated'] += 1
                    results['outdated_entries'].append({
                        'ccd_code': ccd_code,
                        'wwpdb_date': wwpdb_date_str,
                        'ccp4_date': ccp4_date_str,
                        'days_behind': (wwpdb_date - ccp4_date).days,
                        'overall_identical': row.get('overall_identical', '')
                    })
                elif ccp4_date > wwpdb_date:
                    results['date_comparison']['ccp4_up_to_date'] += 1
                else:
                    results['date_comparison']['dates_equal'] += 1
                    
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Sort outdated entries by days behind (most outdated first)
    results['outdated_entries'].sort(key=lambda x: x['days_behind'], reverse=True)
    
    return results


def print_report(results: Dict, output_file: str = None):
    """Print analysis report.
    
    Args:
        results: Analysis results dictionary
        output_file: Optional file path to write report to
    """
    output_lines = []
    
    def add_line(text: str = ''):
        output_lines.append(text)
        print(text)
    
    add_line("=" * 80)
    add_line("COMPARISON RESULTS ANALYSIS REPORT")
    add_line("=" * 80)
    add_line()
    
    # Summary statistics
    add_line("SUMMARY STATISTICS")
    add_line("-" * 80)
    add_line(f"Total entries: {results['total_entries']:,}")
    add_line()
    
    # Overall identical statistics
    add_line("OVERALL IDENTITY")
    add_line("-" * 80)
    total_overall = sum(results['overall_identical'].values())
    if total_overall > 0:
        for status in ['Y', 'N']:
            count = results['overall_identical'][status]
            percentage = (count / total_overall) * 100
            status_label = "Identical" if status == 'Y' else "Different"
            add_line(f"  {status_label}: {count:,} ({percentage:.2f}%)")
    add_line()
    
    # Field-by-field identity statistics
    add_line("FIELD-BY-FIELD IDENTITY")
    add_line("-" * 80)
    fields = [
        ('name_identical', 'Name'),
        ('type_identical', 'Type'),
        ('atom_identical', 'Atom'),
        ('bond_identical', 'Bond'),
        ('descriptor_identical', 'Descriptor')
    ]
    
    for field_key, field_label in fields:
        y_count = results['identity_counts'].get(f'{field_key}_Y', 0)
        n_count = results['identity_counts'].get(f'{field_key}_N', 0)
        total = y_count + n_count
        if total > 0:
            y_pct = (y_count / total) * 100
            n_pct = (n_count / total) * 100
            add_line(f"  {field_label:15s}: Identical={y_count:6,} ({y_pct:5.2f}%), "
                    f"Different={n_count:6,} ({n_pct:5.2f}%)")
    add_line()
    
    # Date comparison statistics
    add_line("DATE COMPARISON (CCP4 vs WWPDB)")
    add_line("-" * 80)
    date_comp = results['date_comparison']
    total_dated = (date_comp['ccp4_outdated'] + date_comp['ccp4_up_to_date'] + 
                   date_comp['dates_equal'])
    
    if total_dated > 0:
        outdated_pct = (date_comp['ccp4_outdated'] / total_dated) * 100
        uptodate_pct = (date_comp['ccp4_up_to_date'] / total_dated) * 100
        equal_pct = (date_comp['dates_equal'] / total_dated) * 100
        
        add_line(f"  CCP4 outdated (ccp4_date < wwpdb_date): "
                f"{date_comp['ccp4_outdated']:,} ({outdated_pct:.2f}%)")
        add_line(f"  CCP4 up-to-date (ccp4_date > wwpdb_date): "
                f"{date_comp['ccp4_up_to_date']:,} ({uptodate_pct:.2f}%)")
        add_line(f"  Dates equal: {date_comp['dates_equal']:,} ({equal_pct:.2f}%)")
    
    if date_comp['missing_dates'] > 0:
        add_line(f"  Missing dates: {date_comp['missing_dates']:,}")
    add_line()
    
    # Outdated entries
    outdated = results['outdated_entries']
    if outdated:
        add_line("OUTDATED CCP4 FILES (ccp4_modified_date < wwpdb_modified_date)")
        add_line("-" * 80)
        add_line(f"Total outdated entries: {len(outdated):,}")
        add_line()
        
        # Show top 20 most outdated
        add_line("Top 20 most outdated entries:")
        add_line(f"{'CCD Code':<12} {'WWPDB Date':<12} {'CCP4 Date':<12} "
                f"{'Days Behind':<12} {'Overall Identical':<18}")
        add_line("-" * 80)
        
        for entry in outdated[:20]:
            add_line(f"{entry['ccd_code']:<12} {entry['wwpdb_date']:<12} "
                    f"{entry['ccp4_date']:<12} {entry['days_behind']:<12,} "
                    f"{entry['overall_identical']:<18}")
        
        if len(outdated) > 20:
            add_line(f"\n... and {len(outdated) - 20:,} more outdated entries")
        
        # Statistics on outdated entries
        if outdated:
            days_behind = [e['days_behind'] for e in outdated]
            avg_days = sum(days_behind) / len(days_behind)
            max_days = max(days_behind)
            min_days = min(days_behind)
            
            add_line()
            add_line("Outdated entries statistics:")
            add_line(f"  Average days behind: {avg_days:.1f}")
            add_line(f"  Maximum days behind: {max_days:,}")
            add_line(f"  Minimum days behind: {min_days:,}")
            
            # Count how many outdated entries are also different
            outdated_different = sum(1 for e in outdated if e['overall_identical'] == 'N')
            outdated_identical = len(outdated) - outdated_different
            add_line()
            add_line("Outdated entries by identity status:")
            add_line(f"  Outdated and different: {outdated_different:,} "
                    f"({(outdated_different/len(outdated)*100):.2f}%)")
            add_line(f"  Outdated but identical: {outdated_identical:,} "
                    f"({(outdated_identical/len(outdated)*100):.2f}%)")
    else:
        add_line("No outdated CCP4 files found.")
    
    add_line()
    add_line("=" * 80)
    
    # Write to file
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
            print(f"\nReport written to: {output_file}")
        except Exception as e:
            print(f"\nWarning: Could not write report to file: {e}", file=sys.stderr)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Analyze comparison results CSV file and generate statistics report.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_comparison_results.py comparison_results_20260107_215221.csv
  python analyze_comparison_results.py comparison_results.csv -o custom_report.txt
        """
    )
    
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to the comparison results CSV file'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default=None,
        help='Optional output file path for the report (default: auto-generate with timestamp)'
    )
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # Generate automatic output filename if not specified
    if args.output is None:
        input_path = Path(args.input_file)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Create output filename: analysis_report_<input_stem>_<timestamp>.txt
        output_filename = f"analysis_report_{input_path.stem}_{timestamp}.txt"
        args.output = output_filename
    
    # Analyze the file
    print(f"Analyzing: {args.input_file}...", file=sys.stderr)
    results = analyze_comparison_results(args.input_file)
    
    # Print report
    print_report(results, args.output)


if __name__ == '__main__':
    main()

