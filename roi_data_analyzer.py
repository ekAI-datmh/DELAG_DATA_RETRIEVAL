#!/usr/bin/env python3
"""
ROI Data Analyzer
Analyzes and visualizes satellite data from ROI folders containing *_ndvi8days, era5, lst, and *_rvi8days subfolders.
Plots bands side by side in 10-day groups and checks value ranges and NaN values.
"""

import os
import glob
import numpy as np
import rasterio
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import argparse
import warnings
warnings.filterwarnings('ignore', category=rasterio.errors.NotGeoreferencedWarning)


def extract_date_from_filename(filename):
    """
    Extract date from filename with format *_YYYY-MM-DD.tif
    
    Args:
        filename (str): Filename to extract date from
        
    Returns:
        datetime or None: Extracted date or None if parsing fails
    """
    try:
        # Extract the date part (YYYY-MM-DD) from filename
        basename = os.path.basename(filename)
        # Split by underscore and find the part that looks like a date
        parts = basename.split('_')
        for part in parts:
            if part.endswith('.tif'):
                part = part[:-4]  # Remove .tif extension
            try:
                # Try to parse as YYYY-MM-DD
                return datetime.strptime(part, '%Y-%m-%d')
            except ValueError:
                continue
        return None
    except Exception as e:
        print(f"Error extracting date from {filename}: {e}")
        return None


def group_files_by_10day_periods(files_with_dates):
    """
    Group files into 10-day periods for plotting
    
    Args:
        files_with_dates (list): List of tuples (filepath, date)
        
    Returns:
        list: List of groups, each containing files from a 10-day period
    """
    if not files_with_dates:
        return []
    
    # Sort by date
    files_with_dates.sort(key=lambda x: x[1])
    
    groups = []
    current_group = []
    
    for i, (filepath, date) in enumerate(files_with_dates):
        if not current_group:
            current_group.append((filepath, date))
        else:
            # Check if this file should start a new group (every 10 files)
            if len(current_group) >= 10:
                groups.append(current_group)
                current_group = [(filepath, date)]
            else:
                current_group.append((filepath, date))
    
    # Add the last group if it has files
    if current_group:
        groups.append(current_group)
    
    return groups


def read_band_data(filepath, band_index=1):
    """
    Read specific band data from a TIFF file
    
    Args:
        filepath (str): Path to the TIFF file
        band_index (int): Band index to read (1-based)
        
    Returns:
        tuple: (data_array, metadata_dict)
    """
    try:
        with rasterio.open(filepath) as dataset:
            if band_index > dataset.count:
                print(f"Warning: Band {band_index} not found in {filepath} (has {dataset.count} bands)")
                return None, None
            
            data = dataset.read(band_index)
            metadata = {
                'width': dataset.width,
                'height': dataset.height,
                'crs': dataset.crs,
                'transform': dataset.transform,
                'nodata': dataset.nodata
            }
            return data, metadata
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, None


def preprocess_data(data, data_type):
    """
    Preprocess data by applying data-type specific filters
    
    Args:
        data (numpy.ndarray): Band data array
        data_type (str): Type of data (ndvi8days, rvi8days, lst, era5)
        
    Returns:
        numpy.ndarray: Preprocessed data with invalid values set to NaN
    """
    if data is None:
        return None
    
    # Create a copy to avoid modifying original data
    processed_data = data.copy()
    
    # Apply data-type specific filters
    if data_type == 'ndvi8days':
        # For NDVI: change -100 or below to NaN
        processed_data[processed_data <= -100] = np.nan
        print(f"    [PREPROCESS] NDVI: Set {np.sum(data <= -100)} pixels (<= -100) to NaN")
        
    elif data_type == 'lst':
        # For LST: change 0 or below to NaN
        processed_data[processed_data <= 0] = np.nan
        print(f"    [PREPROCESS] LST: Set {np.sum(data <= 0)} pixels (<= 0) to NaN")
    
    return processed_data


def analyze_band_values(data, filepath, band_index, data_type):
    """
    Analyze band values: check for NaN, print value ranges
    
    Args:
        data (numpy.ndarray): Band data array
        filepath (str): Path to the source file
        band_index (int): Band index
        data_type (str): Type of data (ndvi8days, rvi8days, etc.)
    """
    if data is None:
        print(f"  [{data_type}] {os.path.basename(filepath)} - Band {band_index}: No data")
        return
    
    # Check for NaN values
    nan_count = np.isnan(data).sum()
    total_pixels = data.size
    nan_percentage = (nan_count / total_pixels) * 100 if total_pixels > 0 else 0
    
    # Get value range (excluding NaN)
    valid_data = data[~np.isnan(data)]
    if len(valid_data) > 0:
        min_val = np.min(valid_data)
        max_val = np.max(valid_data)
        mean_val = np.mean(valid_data)
        std_val = np.std(valid_data)
    else:
        min_val = max_val = mean_val = std_val = np.nan
    
    print(f"  [{data_type}] {os.path.basename(filepath)} - Band {band_index}:")
    print(f"    Range: [{min_val:.6f}, {max_val:.6f}], Mean: {mean_val:.6f}, Std: {std_val:.6f}")
    print(f"    NaN pixels: {nan_count}/{total_pixels} ({nan_percentage:.2f}%)")


def create_side_by_side_plot(group_data, group_index, output_dir, roi_name):
    """
    Create side-by-side plots for a group of files
    
    Args:
        group_data (dict): Dictionary with data_type as keys and list of (filepath, date, data) as values
        group_index (int): Index of the current group
        output_dir (str): Output directory for saving plots
        roi_name (str): Name of the ROI
    """
    # Determine the number of files in this group
    max_files = max(len(files) for files in group_data.values() if files)
    
    if max_files == 0:
        print(f"No data to plot for group {group_index}")
        return
    
    # Create figure with subplots
    data_types = ['lst', 'ndvi8days', 'rvi_8days', 'era5']
    fig, axes = plt.subplots(len(data_types), max_files, 
                            figsize=(3*max_files, 3*len(data_types)))
    
    # Ensure axes is 2D
    if max_files == 1:
        axes = axes.reshape(-1, 1)
    elif len(data_types) == 1:
        axes = axes.reshape(1, -1)
    
    fig.suptitle(f'ROI: {roi_name} - Group {group_index + 1} (10-day period)', fontsize=16, fontweight='bold')
    
    for row, data_type in enumerate(data_types):
        files_data = group_data.get(data_type, [])
        
        for col in range(max_files):
            ax = axes[row, col]
            
            if col < len(files_data):
                filepath, date, data = files_data[col]
                
                if data is not None:
                    # Plot the data
                    im = ax.imshow(data, cmap='viridis', aspect='auto')
                    ax.set_title(f'{data_type}\n{date.strftime("%Y-%m-%d")}', fontsize=10)
                    
                    # Add colorbar
                    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
                else:
                    ax.text(0.5, 0.5, 'No Data', ha='center', va='center', transform=ax.transAxes)
                    ax.set_title(f'{data_type}\nNo Data', fontsize=10)
            else:
                ax.text(0.5, 0.5, 'No File', ha='center', va='center', transform=ax.transAxes)
                ax.set_title(f'{data_type}\nNo File', fontsize=10)
            
            ax.axis('off')
    
    plt.tight_layout()
    
    # Save the plot
    output_filename = f'{roi_name}_group_{group_index + 1:02d}.png'
    output_path = os.path.join(output_dir, output_filename)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Saved plot: {output_path}")


def find_data_folder(roi_folder_path, folder_pattern):
    """
    Find a folder that matches the given pattern in the ROI folder
    
    Args:
        roi_folder_path (Path): Path to the ROI folder
        folder_pattern (str): Pattern to match (e.g., '*_ndvi8days')
        
    Returns:
        Path or None: Path to the found folder or None if not found
    """
    matching_folders = list(roi_folder_path.glob(folder_pattern))
    if matching_folders:
        return matching_folders[0]  # Return the first match
    return None


def analyze_roi_folder(roi_folder_path, output_dir=None):
    """
    Analyze a single ROI folder containing *_ndvi8days, era5, lst, and *_rvi8days subfolders
    
    Args:
        roi_folder_path (str): Path to the ROI folder
        output_dir (str): Output directory for saving plots (optional)
    """
    roi_folder_path = Path(roi_folder_path)
    roi_name = roi_folder_path.name
    
    if output_dir is None:
        output_dir = roi_folder_path / 'analysis_plots'
    else:
        output_dir = Path(output_dir)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"ANALYZING ROI: {roi_name}")
    print(f"{'='*60}")
    
    # Define data types and their corresponding folder patterns and band indices
    data_config = {
        'lst': {'band': 1, 'pattern': '*_????-??-??.tif', 'folder_pattern': 'lst'},
        'ndvi8days': {'band': 1, 'pattern': '*_????-??-??.tif', 'folder_pattern': '*_ndvi8days'},
        'rvi_8days': {'band': 1, 'pattern': '*_????-??-??.tif', 'folder_pattern': '*_rvi_8days'},
        'era5': {'band': 1, 'pattern': '*_????-??-??.tif', 'folder_pattern': 'era5'}
    }
    
    # Collect all files from each data type
    all_data = {}
    
    for data_type, config in data_config.items():
        # Find the actual folder using pattern matching
        if config['folder_pattern'].startswith('*'):
            data_folder = find_data_folder(roi_folder_path, config['folder_pattern'])
        else:
            data_folder = roi_folder_path / config['folder_pattern']
            if not data_folder.exists():
                data_folder = None
        
        print(f"\n--- Processing {data_type} ---")
        
        if data_folder is None:
            print(f"Warning: {data_type} folder (pattern: {config['folder_pattern']}) not found in {roi_folder_path}")
            all_data[data_type] = []
            continue
        
        print(f"Found {data_type} folder: {data_folder.name}")
        
        if not data_folder.exists():
            print(f"Warning: {data_type} folder not found at {data_folder}")
            all_data[data_type] = []
            continue
        
        # Find all TIFF files
        tif_pattern = str(data_folder / config['pattern'])
        tif_files = glob.glob(tif_pattern)
        
        if not tif_files:
            print(f"No TIFF files found in {data_folder}")
            all_data[data_type] = []
            continue
        
        print(f"Found {len(tif_files)} files in {data_folder}")
        
        # Extract dates and read data
        files_with_data = []
        for tif_file in sorted(tif_files):
            date = extract_date_from_filename(tif_file)
            if date is None:
                print(f"Warning: Could not extract date from {tif_file}")
                continue
            
            # Read band data
            data, metadata = read_band_data(tif_file, config['band'])
            
            # Preprocess data (apply filters for NDVI and LST)
            processed_data = preprocess_data(data, data_type)
            
            # Analyze the preprocessed data
            analyze_band_values(processed_data, tif_file, config['band'], data_type)
            
            files_with_data.append((tif_file, date, processed_data))
        
        all_data[data_type] = files_with_data
    
    # Group files by 10-day periods and create plots
    print(f"\n--- Creating 10-day period plots ---")
    
    # Find the maximum number of groups across all data types
    max_groups = 0
    grouped_data = {}
    
    for data_type, files_with_data in all_data.items():
        if not files_with_data:
            grouped_data[data_type] = []
            continue
        
        # Extract just filepath and date for grouping
        files_with_dates = [(filepath, date) for filepath, date, data in files_with_data]
        groups = group_files_by_10day_periods(files_with_dates)
        
        # Add data back to groups
        data_dict = {(filepath, date): data for filepath, date, data in files_with_data}
        groups_with_data = []
        for group in groups:
            group_with_data = []
            for filepath, date in group:
                data = data_dict.get((filepath, date))
                group_with_data.append((filepath, date, data))
            groups_with_data.append(group_with_data)
        
        grouped_data[data_type] = groups_with_data
        max_groups = max(max_groups, len(groups_with_data))
    
    # Create plots for each group
    for group_idx in range(max_groups):
        print(f"\nCreating plot for group {group_idx + 1}/{max_groups}")
        
        # Prepare data for this group
        group_data = {}
        for data_type, groups in grouped_data.items():
            if group_idx < len(groups):
                group_data[data_type] = groups[group_idx]
            else:
                group_data[data_type] = []
        
        create_side_by_side_plot(group_data, group_idx, output_dir, roi_name)
    
    print(f"\nAnalysis complete for {roi_name}")
    print(f"Plots saved in: {output_dir}")


def main():
    """
    Main function to run the ROI data analyzer
    """
    parser = argparse.ArgumentParser(description='Analyze ROI satellite data and create visualizations')
    parser.add_argument('roi_folder', help='Path to the ROI folder containing data subfolders')
    parser.add_argument('--output-dir', '-o', help='Output directory for plots (default: roi_folder/analysis_plots)')
    
    args = parser.parse_args()
    
    # Validate input
    if not os.path.exists(args.roi_folder):
        print(f"Error: ROI folder not found: {args.roi_folder}")
        return
    
    if not os.path.isdir(args.roi_folder):
        print(f"Error: Path is not a directory: {args.roi_folder}")
        return
    
    # Run analysis
    try:
        analyze_roi_folder(args.roi_folder, args.output_dir)
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 