import os
import argparse
import glob
import numpy as np
import rasterio
from rasterio.errors import RasterioIOError

def filter_image_by_range(file_path: str, lower_bound: float, upper_bound: float) -> None:
    """
    Opens a GeoTIFF, filters its first band based on a valid range,
    and overwrites the file with the filtered data.

    Pixels with values outside the [lower_bound, upper_bound] range are
    set to np.nan.

    Args:
        file_path: The full path to the GeoTIFF file.
        lower_bound: The minimum valid value for a pixel.
        upper_bound: The maximum valid value for a pixel.
    """
    try:
        with rasterio.open(file_path, 'r+') as src:
            # Ensure the file is writable
            if src.mode != 'r+':
                print(f"Warning: Cannot write to {file_path}. It is not in update mode.")
                return

            # Preserve original nodata value if it exists (assumed common for all bands)
            nodata_val = src.nodata

            total_changed = 0  # Track total modified pixels across bands

            for band_idx in range(1, src.count + 1):
                band_data = src.read(band_idx).astype('float32')

                # Mask of values outside allowed range
                mask = (band_data < lower_bound) | (band_data > upper_bound)
                pixels_to_change = np.sum(mask)

                if pixels_to_change > 0:
                    band_data[mask] = np.nan
                    src.write(band_data, band_idx)
                    total_changed += pixels_to_change

            if total_changed > 0:
                # Ensure nodata is set to NaN
                if not (nodata_val is not None and np.isnan(nodata_val)):
                    src.nodata = np.nan
                print(f"  > {os.path.basename(file_path)}: Replaced {total_changed} pixels outside range [{lower_bound}, {upper_bound}] across {src.count} band(s).")
            else:
                print(f"  Skipping {os.path.basename(file_path)}: All pixel values are within the valid range across all bands.")

    except RasterioIOError as e:
        print(f"Error opening or processing {file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred with {file_path}: {e}")


def main(lst_folder: str) -> None:
    """
    Main function to find all .tif files in a folder and apply a value filter.
    """
    print(f"Starting to process files in: {lst_folder}")
    print("Filtering pixel values to be within the range [260, 340].")
    
    tif_files = glob.glob(os.path.join(lst_folder, '*.tif'))
    
    if not tif_files:
        print("No .tif files found in the specified folder.")
        return

    print(f"Found {len(tif_files)} .tif files to process.\n")
    
    for file_path in tif_files:
        filter_image_by_range(file_path, lower_bound=260, upper_bound=340)
        
    print("\nProcessing complete.")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser(
    #     description="Filter LST images in a folder by replacing pixel values outside the 260-340 range with NaN."
    # )
    # parser.add_argument(
    #     '--lst_folder',
    #     required=True,
    #     help="The path to the folder containing the LST .tif files to be processed."
    # )
    # args = parser.parse_args()
    
    for ROI in os.listdir('/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/data_grid_base'):
        for folder in os.listdir(os.path.join('/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/data_grid_base', ROI)):
            if os.path.isdir(os.path.join('/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/data_grid_base', ROI, folder)):
                if "lst" in folder:
                    print(f"Processing {os.path.join('/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/data_grid_base', ROI, folder)}")
                    main(os.path.join('/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/data_grid_base', ROI, folder)) 