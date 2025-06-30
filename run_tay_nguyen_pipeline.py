#!/usr/bin/env python3
"""
Pipeline script to:
1. Filter Tay Nguyen grids based on land cover criteria
2. Use the filtered results to crawl satellite data

Usage: python run_tay_nguyen_pipeline.py
"""

import os
import sys
import subprocess
import time
import datetime
import re

def run_command(command, description):
    """
    Run a command and handle errors
    """
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"{'='*60}")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with return code {e.returncode}")
        print("STDOUT:")
        print(e.stdout)
        print("STDERR:")  
        print(e.stderr)
        return False

def check_file_exists(filepath, description):
    """
    Check if a file exists and print status
    """
    if os.path.exists(filepath):
        print(f"✅ {description} found: {filepath}")
        return True
    else:
        print(f"❌ {description} not found: {filepath}")
        return False

def main():
    # Create log file with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"tay_nguyen_pipeline_{timestamp}.log"
    
    print("🚀 Starting Tay Nguyen Data Retrieval Pipeline")
    print(f"Working directory: {os.getcwd()}")
    print(f"📝 Log file: {log_file}")
    
    # Write initial log entry
    with open(log_file, 'w') as f:
        f.write(f"Tay Nguyen Pipeline Started: {datetime.datetime.now()}\n")
        f.write(f"Working directory: {os.getcwd()}\n")
        f.write("="*60 + "\n")
    
    # Check test mode status
    print("\n" + "="*60)
    print("🧪 TEST MODE STATUS CHECK")
    print("="*60)
    
    # Check filter_tay_nguyen_grids.py for test mode
    try:
        with open('filter_tay_nguyen_grids.py', 'r') as f:
            content = f.read()
            if 'TEST_MODE = True' in content:
                print("✅ Grid filtering: TEST MODE ENABLED (will process limited grids)")
            else:
                print("⚠️  Grid filtering: FULL MODE (will process all grids)")

            # Check for grid sampling configuration
            match = re.search(r"NUM_SAMPLED_GRIDS\s*=\s*(\d+)", content)
            if match:
                num_grids = match.group(1)
                print(f"✅ Grid filtering: Configured to sample {num_grids} diverse grids.")
            else:
                print("⚠️  Grid filtering: Set to use ALL qualifying grids (no sampling).")
    except:
        print("❓ Could not check grid filtering test mode status")
    
    # Check main.py for test mode
    try:
        with open('main.py', 'r') as f:
            content = f.read()
            if 'TEST_MODE_CRAWLING = True' in content:
                print("✅ Data crawling: TEST MODE ENABLED (will process limited ROIs)")
            else:
                print("⚠️  Data crawling: FULL MODE (will process all ROIs)")
    except:
        print("❓ Could not check data crawling test mode status")
    
    # Step 1: Filter grids in Tay Nguyen region
    print("\n" + "="*60)
    print("STEP 1: Filtering Tay Nguyen grids based on land cover criteria")
    print("="*60)
    
    if not run_command("python3 filter_tay_nguyen_grids.py", "Grid filtering"):
        error_msg = "❌ Pipeline failed at grid filtering step."
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(f"{error_msg} {datetime.datetime.now()}\n")
        sys.exit(1)
    
    # Check if CSV file was generated
    csv_file = "tay_nguyen_filtered_grids.csv"
    if not check_file_exists(csv_file, "Filtered grids CSV"):
        print("❌ Pipeline failed: CSV file not generated. Exiting.")
        sys.exit(1)
    
    # Show CSV summary
    try:
        import pandas as pd
        df = pd.read_csv(csv_file)
        print(f"📊 CSV Summary: {len(df)} grids selected for data crawling")
        print(f"   - Grid IDs: {df['grid_id'].tolist()[:5]}{'...' if len(df) > 5 else ''}")
    except Exception as e:
        print(f"⚠️  Could not read CSV summary: {e}")
    
    # Step 2: Crawl satellite data for filtered grids
    print("\n" + "="*60)
    print("STEP 2: Crawling satellite data for filtered grids")
    print("="*60)
    print("⚠️  This step may take a very long time (hours) depending on the number of grids...")
    
    # Automatic execution - no user confirmation required
    print("🚀 Proceeding automatically with data crawling...")
    
    start_time = time.time()
    
    if not run_command("python3 main.py", "Satellite data crawling"):
        error_msg = "❌ Pipeline failed at data crawling step."
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(f"{error_msg} {datetime.datetime.now()}\n")
        sys.exit(1)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n🎉 Pipeline completed successfully!")
    print(f"⏱️  Total crawling time: {duration/60:.1f} minutes")
    print(f"📁 Check the download_data_v3 folder for results")
    
    # Write completion log
    with open(log_file, 'a') as f:
        f.write(f"\nPipeline completed successfully: {datetime.datetime.now()}\n")
        f.write(f"Total crawling time: {duration/60:.1f} minutes\n")
        f.write("="*60 + "\n")

if __name__ == "__main__":
    main() 