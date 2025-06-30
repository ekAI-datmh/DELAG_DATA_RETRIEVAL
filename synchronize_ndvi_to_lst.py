import os
import argparse
import glob
import shutil
from datetime import datetime
import re

def get_image_date_map(folder_path: str) -> dict[datetime, str]:
    """
    Scans a folder for .tif files, extracts dates from filenames,
    and returns a dictionary mapping the date to the file path.
    
    Args:
        folder_path: Path to the folder containing .tif files.

    Returns:
        A dictionary where keys are datetime objects and values are file paths.
    """
    tif_files = glob.glob(os.path.join(folder_path, '*.tif'))
    date_map = {}
    date_pattern = re.compile(r'(\d{4}-\d{2}-\d{2})')

    for file_path in tif_files:
        base = os.path.basename(file_path)
        match = date_pattern.search(base)
        if match:
            try:
                file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
                date_map[file_date] = file_path
            except ValueError:
                print(f"Warning: Could not parse date from filename: {base}")
    return date_map

def find_nearest_image(target_date: datetime, source_map: dict[datetime, str]) -> str | None:
    """
    Finds the file path in the source map with the date closest to the target date.

    Args:
        target_date: The date to match against.
        source_map: A dictionary mapping dates to file paths for the source images.

    Returns:
        The file path of the image with the nearest date, or None if the map is empty.
    """
    if not source_map:
        return None
    
    # Find the date in the source map that has the minimum time difference from the target date
    closest_date = min(
        source_map.keys(),
        key=lambda date: abs(date - target_date)
    )
    return source_map[closest_date]

def main(ndvi_folder: str, lst_folder: str, output_folder: str) -> None:
    """
    Main function to synchronize NDVI images to LST dates.
    """
    print("Starting NDVI to LST date synchronization process...")
    print(f"NDVI source: {ndvi_folder}")
    print(f"LST source (for dates): {lst_folder}")
    print(f"Output destination: {output_folder}")

    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # 1. Get date-to-path maps for both LST and NDVI folders
    lst_date_map = get_image_date_map(lst_folder)
    ndvi_date_map = get_image_date_map(ndvi_folder)

    if not lst_date_map:
        print("Error: No .tif files with valid dates found in the LST folder. Cannot proceed.")
        return
    if not ndvi_date_map:
        print("Error: No .tif files with valid dates found in the NDVI folder. Cannot proceed.")
        return
        
    print(f"\nFound {len(lst_date_map)} target dates from LST folder.")
    print(f"Found {len(ndvi_date_map)} available NDVI images to use.")

    # 2. For each LST date, find the nearest NDVI image and copy it
    for lst_date, lst_path in sorted(lst_date_map.items()):
        
        nearest_ndvi_path = find_nearest_image(lst_date, ndvi_date_map)
        
        if nearest_ndvi_path:
            # Construct the new filename
            # Takes the prefix from the NDVI file and appends the LST date
            original_ndvi_filename = os.path.basename(nearest_ndvi_path)
            # Use regex to replace the date part of the filename
            date_str_to_replace = re.search(r'(\d{4}-\d{2}-\d{2})', original_ndvi_filename).group(1)
            new_filename = original_ndvi_filename.replace(date_str_to_replace, lst_date.strftime('%Y-%m-%d'))
            
            output_path = os.path.join(output_folder, new_filename)

            # Copy the file
            shutil.copy2(nearest_ndvi_path, output_path)
            
            nearest_ndvi_date = find_nearest_image(lst_date, {d:d for d in ndvi_date_map.keys()})
            print(f"  - Target date {lst_date.strftime('%Y-%m-%d')}:")
            print(f"    > Found nearest NDVI image from {nearest_ndvi_date.strftime('%Y-%m-%d')}.")
            print(f"    > Created new file: {new_filename}")
        else:
            print(f"Could not find a nearest NDVI image for LST date {lst_date.strftime('%Y-%m-%d')}")

    print("\nSynchronization complete.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Synchronize NDVI images to match the dates of LST images by finding the nearest available NDVI image for each LST date."
    )
    parser.add_argument(
        '--ndvi_folder',
        required=True,
        help="Path to the folder containing the source NDVI .tif files."
    )
    parser.add_argument(
        '--lst_folder',
        required=True,
        help="Path to the folder containing LST .tif files, which will be used to define the target dates."
    )
    parser.add_argument(
        '--output_folder',
        required=True,
        help="Path to the folder where the new, date-synchronized NDVI .tif files will be saved."
    )
    args = parser.parse_args()
    
    main(args.ndvi_folder, args.lst_folder, args.output_folder) 