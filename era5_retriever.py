import os
import glob
import time
import argparse
from datetime import datetime
from pathlib import Path
import rasterio
from rasterio.warp import transform_bounds
import ee
import requests
import shutil
import zipfile

# --- Global Configuration ---
SKIPPING = True
TARGET_CRS = 'EPSG:4326'
EXPORT_SCALE = 1000  # ERA5 native resolution is much coarser, 1000m is reasonable

# --- Earth Engine Initialization ---
try:
    ee.Initialize(project='ee-hadat-461702-p4')
except Exception:
    print("Authenticating to Earth Engine...")
    ee.Authenticate()
    ee.Initialize(project='ee-hadat-461702-p4')

# --- Utility Functions (Adapted from new_main.py) ---

def get_roi_coords_from_tif(tif_path):
    """Reads bounds from a TIF and converts them to the target CRS."""
    with rasterio.open(tif_path) as dataset:
        bounds = dataset.bounds
        if dataset.crs.to_string() != TARGET_CRS:
            print(f"Transforming bounds from {dataset.crs} to {TARGET_CRS}")
            bounds = transform_bounds(dataset.crs, TARGET_CRS, *bounds)
        
        coordinates = [
            [bounds[0], bounds[1]], [bounds[2], bounds[1]],
            [bounds[2], bounds[3]], [bounds[0], bounds[3]],
            [bounds[0], bounds[1]]
        ]
        return [[float(x), float(y)] for x, y in coordinates]

def get_dates_from_filenames(folder_path):
    """Gets a sorted list of unique dates from .tif filenames in a folder."""
    tif_files = glob.glob(os.path.join(folder_path, '*.tif'))
    dates = set()
    for tif in tif_files:
        base = os.path.basename(tif)
        try:
            date_str = base.split('_')[-1].replace('.tif', '')
            date = datetime.strptime(date_str, '%Y-%m-%d')
            dates.add(date)
        except (ValueError, IndexError):
            print(f"Could not parse date from filename: {base}")
    return sorted(list(dates))

def verify_image(img_path):
    """Verifies that a downloaded image is a valid GeoTIFF."""
    try:
        with rasterio.open(img_path) as src:
            if src.crs and src.width > 0 and src.height > 0:
                print(f"  Verification successful for {os.path.basename(img_path)} (CRS: {src.crs}, Size: {src.width}x{src.height})")
                return True
        print(f"  Verification failed for {os.path.basename(img_path)}: Invalid raster data.")
        return False
    except (RasterioIOError, Exception) as e:
        print(f"  Verification error for {img_path}: {e}")
        return False

def export_ee_image(image, bands, region, out_path, scale, crs=TARGET_CRS):
    """Exports an Earth Engine image to a local path."""
    temp_dir = os.path.join(os.path.dirname(out_path), 'temp_dl')
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        region_geometry = ee.Geometry.Polygon(region, proj=crs, evenOdd=False)
        image = image.clip(region_geometry).select(bands)

        band_info = image.bandNames().getInfo()
        if not band_info:
            print(f"Warning: Image for {out_path} has no bands after clipping. Skipping export.")
            return

        url = image.getDownloadURL({
            'scale': scale, 'region': region, 'fileFormat': 'GeoTIFF', 'crs': crs
        })

        print(f"Attempting download for {os.path.basename(out_path)}...")
        response = requests.get(url, stream=True, timeout=600)
        response.raise_for_status()

        temp_zip_path = os.path.join(temp_dir, 'download.zip')
        with open(temp_zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)

        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            tif_files = [f for f in zip_ref.namelist() if f.endswith('.tif')]

            if not tif_files:
                print("Error: ZIP file did not contain any .tif files.")
                return

            # Extract all tif files so we can combine them into one multi-band image
            for tif in tif_files:
                zip_ref.extract(tif, temp_dir)

            # Order the bands to match the requested `bands` list when possible
            ordered_tifs = []
            for b in bands:
                match = next((t for t in tif_files if b in os.path.basename(t)), None)
                if match:
                    ordered_tifs.append(match)

            # Fallback: if we could not fully match by name, keep the original order
            if len(ordered_tifs) != len(bands):
                ordered_tifs = tif_files

            # Build a multi-band GeoTIFF from the individual single-band files
            first_tif_path = os.path.join(temp_dir, ordered_tifs[0])
            with rasterio.open(first_tif_path) as src0:
                profile = src0.profile
                band_arrays = [src0.read(1)]

            for tif in ordered_tifs[1:]:
                with rasterio.open(os.path.join(temp_dir, tif)) as src:
                    band_arrays.append(src.read(1))

            profile.update(count=len(band_arrays))

            with rasterio.open(out_path, 'w', **profile) as dst:
                for idx, arr in enumerate(band_arrays, start=1):
                    dst.write(arr, idx)

            print(f"Successfully downloaded and merged {len(band_arrays)} bands into {out_path}")
    except Exception as e:
        print(f"Error during export for {out_path}: {e}")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def resample_to_match_reference(source_path, reference_path):
    """
    Resamples a source GeoTIFF to match the metadata (CRS, transform, dimensions)
    of a reference GeoTIFF. This ensures the images are perfectly aligned.
    """
    try:
        with rasterio.open(reference_path) as ref:
            ref_meta = ref.meta.copy()

        with rasterio.open(source_path) as src:
            # Check if resampling is actually needed
            if (src.width == ref_meta['width'] and 
                src.height == ref_meta['height'] and 
                src.transform == ref_meta['transform']):
                print(f"  > Alignment for {os.path.basename(source_path)} is already correct. No resampling needed.")
                return

            print(f"  > Resampling {os.path.basename(source_path)} to match reference grid...")
            
            # Update the metadata for the output file
            ref_meta.update({
                'count': src.count, # Match the band count of the source
                'dtype': src.meta['dtype'], # Match the data type of the source
                'nodata': src.nodata # Preserve nodata value
            })

            # Create a temporary file for the resampled output
            temp_output_path = source_path + ".resampled.tif"

            with rasterio.open(temp_output_path, 'w', **ref_meta) as dst:
                for i in range(1, src.count + 1):
                    rasterio.warp.reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=ref_meta['transform'],
                        dst_crs=ref_meta['crs'],
                        resampling=rasterio.warp.Resampling.bilinear # Good for continuous data like temperature
                    )
            
            # Replace the original source file with the new resampled file
            shutil.move(temp_output_path, source_path)
            print(f"  > Successfully resampled and replaced {os.path.basename(source_path)}")

    except Exception as e:
        print(f"  > Error during resampling for {source_path}: {e}")

# --- Core ERA5 Function ---

def get_era5_for_date(target_date, roi_geom, region, out_folder, reference_tif_path):
    """
    Fetches and exports the closest ERA5 Land image for a specific date.
    It downloads two key bands: 2m air temperature and skin temperature.
    After download, it resamples the image to match the reference TIF.
    """
    date_str = target_date.strftime('%Y-%m-%d')
    out_path = os.path.join(out_folder, f'ERA5_data_{date_str}.tif')
    
    # 1. Check if the file already exists and skip if it does
    if SKIPPING and os.path.exists(out_path):
        print(f"ERA5 file for {date_str} already exists. Verifying and skipping download.")
        verify_image(out_path)
        # Even if skipped, ensure it's aligned
        print(f"  > Checking alignment of existing file: {os.path.basename(out_path)}")
        resample_to_match_reference(out_path, reference_tif_path)
        return

    try:
        # Search within a +/- 1-day window to ensure we find the exact day's image
        start = ee.Date(target_date).advance(-1, 'day')
        end = ee.Date(target_date).advance(1, 'day')
        
        era5_collection = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR') \
            .filterDate(start, end) \
            .filterBounds(roi_geom)

        if era5_collection.size().getInfo() == 0:
            print(f"No ERA5 images found for {date_str}. Skipping.")
            return
            
        # The daily aggregate should have one image per day, so we can take the first.
        best_img = ee.Image(era5_collection.first())
        
        print(f"Exporting ERA5 data for {date_str}...")
        export_ee_image(
            image=best_img,
            bands=['temperature_2m', 'skin_temperature'],
            region=region,
            out_path=out_path,
            scale=EXPORT_SCALE,
            crs=TARGET_CRS
        )
        time.sleep(0.5)  # Pause to avoid overwhelming the server

        # 2. Resample the newly downloaded image to match the reference
        if os.path.exists(out_path):
            resample_to_match_reference(out_path, reference_tif_path)

    except ee.EEException as e:
        print(f"EE Error fetching ERA5 for {date_str}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for {date_str}: {e}")

# --- Main Execution Logic ---

def main(input_folder, output_folder):
    """
    Main function to drive the ERA5 data retrieval process.
    """
    print(f"Starting ERA5 data retrieval based on LST files in: {input_folder}")
    print(f"Output will be saved to: {output_folder}")
    os.makedirs(output_folder, exist_ok=True)

    # 1. Get a sample TIF file to define the ROI
    sample_tif_path = next(Path(input_folder).glob('*.tif'), None)
    if not sample_tif_path:
        print(f"Error: No .tif files found in '{input_folder}'. Cannot determine ROI.")
        return
    print(f"Using sample file for ROI and reference grid: {sample_tif_path.name}")

    # 2. Define ROI from the sample TIF
    try:
        coords = get_roi_coords_from_tif(str(sample_tif_path))
        roi_geom = ee.Geometry.Polygon(coords, proj=TARGET_CRS, evenOdd=False)
        print(f"Successfully defined ROI geometry using {TARGET_CRS}.")
    except Exception as e:
        print(f"Error defining ROI from {sample_tif_path.name}: {e}")
        return

    # 3. Get all unique dates from the LST filenames
    target_dates = get_dates_from_filenames(input_folder)
    if not target_dates:
        print("Error: Could not find any valid dates from filenames in the LST folder.")
        return
    print(f"Found {len(target_dates)} unique dates to process.")

    # 4. Loop through each date and download ERA5 data
    for date in target_dates:
        print(f"\n--- Processing Date: {date.strftime('%Y-%m-%d')} ---")
        get_era5_for_date(date, roi_geom, coords, output_folder, str(sample_tif_path))
    
    # 5. Final verification of all downloaded files
    print("\n--- Final Verification Pass ---")
    downloaded_files = glob.glob(os.path.join(output_folder, '*.tif'))
    for f in downloaded_files:
        verify_image(f)
        
    print("\nERA5 data retrieval process complete.")


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description="Download ERA5 data (2m temperature and skin temperature) for specific dates and regions defined by a folder of LST GeoTIFF files."
#     )
#     parser.add_argument(
#         '--input_folder',
#         required=True,
#         help="Path to the folder containing LST .tif files. These files define the dates and ROI for the ERA5 data download."
#     )
#     parser.add_argument(
#         '--output_folder',
#         required=True,
#         help="Path to the folder where the downloaded ERA5 .tif files will be saved."
#     )
#     args = parser.parse_args()
    
#     main(args.input_folder, args.output_folder) 