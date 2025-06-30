#!/usr/bin/env python3
"""
Example script for using the ROI Data Analyzer
Demonstrates batch processing of multiple ROI folders
"""

import os
import sys
from pathlib import Path
from roi_data_analyzer import analyze_roi_folder


def batch_analyze_rois(base_folder, output_base_dir=None):
    """
    Batch analyze multiple ROI folders
    
    Args:
        base_folder (str): Base folder containing multiple ROI subdirectories
        output_base_dir (str): Base directory for output plots (optional)
    """
    base_folder = Path(base_folder)
    
    if not base_folder.exists():
        print(f"Error: Base folder not found: {base_folder}")
        return
    
    # Find all ROI subdirectories
    roi_folders = [d for d in base_folder.iterdir() if d.is_dir()]
    
    if not roi_folders:
        print(f"No subdirectories found in {base_folder}")
        return
    
    print(f"Found {len(roi_folders)} ROI folders to analyze:")
    for roi_folder in roi_folders:
        print(f"  - {roi_folder.name}")
    
    # Process each ROI
    for i, roi_folder in enumerate(roi_folders, 1):
        print(f"\n{'='*80}")
        print(f"Processing ROI {i}/{len(roi_folders)}: {roi_folder.name}")
        print(f"{'='*80}")
        
        try:
            # Set output directory for this ROI
            if output_base_dir:
                output_dir = Path(output_base_dir) / f"{roi_folder.name}_analysis"
            else:
                output_dir = None
            
            # Analyze this ROI
            analyze_roi_folder(str(roi_folder), str(output_dir) if output_dir else None)
            
        except Exception as e:
            print(f"Error analyzing {roi_folder.name}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*80}")
    print("Batch analysis complete!")
    print(f"{'='*80}")


def example_single_roi():
    """
    Example of analyzing a single ROI folder
    """
    # Example ROI folder path (modify this to your actual path)
    roi_folder = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/download_data_v3/TayNguyen_001"
    output_dir = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/analysis_output"
    
    print("Example: Analyzing single ROI folder")
    print(f"ROI folder: {roi_folder}")
    print(f"Output directory: {output_dir}")
    
    if os.path.exists(roi_folder):
        analyze_roi_folder(roi_folder, output_dir)
    else:
        print(f"ROI folder not found: {roi_folder}")
        print("Please update the 'roi_folder' path in this example to point to your actual ROI folder.")


def example_batch_analysis():
    """
    Example of batch analyzing multiple ROI folders
    """
    # Example base folder containing multiple ROI subdirectories
    base_folder = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/download_data_v3"
    output_base_dir = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/batch_analysis_output"
    
    print("Example: Batch analyzing multiple ROI folders")
    print(f"Base folder: {base_folder}")
    print(f"Output base directory: {output_base_dir}")
    
    if os.path.exists(base_folder):
        batch_analyze_rois(base_folder, output_base_dir)
    else:
        print(f"Base folder not found: {base_folder}")
        print("Please update the 'base_folder' path in this example to point to your actual data folder.")


def main():
    """
    Main function with usage examples
    """
    print("ROI Data Analyzer - Example Usage")
    print("="*50)
    
    print("\nAvailable examples:")
    print("1. Analyze single ROI folder")
    print("2. Batch analyze multiple ROI folders")
    print("3. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-3): ").strip()
            
            if choice == '1':
                example_single_roi()
                break
            elif choice == '2':
                example_batch_analysis()
                break
            elif choice == '3':
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main() 