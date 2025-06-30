import os
import argparse
import glob
import numpy as np
import rasterio
from rasterio.errors import RasterioIOError

def check_for_nan(file_path: str) -> bool:
    """
    Checks if any band in a GeoTIFF file contains NaN values.

    Args:
        file_path: The full path to the GeoTIFF file.

    Returns:
        True if NaN values are found, False otherwise.
    """
    try:
        with rasterio.open(file_path) as src:
            # Check each band for NaN values
            for i in range(1, src.count + 1):
                band_data = src.read(i)
                if np.isnan(band_data).any():
                    return True
        return False
    except RasterioIOError as e:
        print(f"  > Error reading {os.path.basename(file_path)}: {e}")
        return False
    except Exception as e:
        print(f"  > An unexpected error occurred with {os.path.basename(file_path)}: {e}")
        return False

def main(folder_path: str) -> None:
    """
    Main function to scan a folder for .tif files and check for NaN values.
    """
    print(f"Starting NaN value check in folder: {folder_path}")
    
    tif_files = glob.glob(os.path.join(folder_path, '*.tif'))
    
    if not tif_files:
        print("No .tif files found in the specified folder.")
        return

    print(f"Found {len(tif_files)} .tif files to check.\n")
    
    nan_found_count = 0
    
    for file_path in sorted(tif_files):
        has_nan = check_for_nan(file_path)
        if has_nan:
            print(f"  [!] NaN values FOUND in: {os.path.basename(file_path)}")
            nan_found_count += 1
        else:
            print(f"  [✓] No NaN values in:    {os.path.basename(file_path)}")
            
    print("\n--- Check Complete ---")
    if nan_found_count > 0:
        print(f"Summary: Found {nan_found_count} out of {len(tif_files)} files with NaN values.")
    else:
        print(f"Summary: All {len(tif_files)} files are free of NaN values.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Check all .tif images in a folder for the presence of NaN values."
    )
    parser.add_argument(
        '--folder_path',
        required=True,
        help="The path to the folder containing the .tif files to be checked."
    )
    args = parser.parse_args()
    
    main(args.folder_path) 