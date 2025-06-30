import ee
import requests  # used to download files
import os
import tempfile
import zipfile
import shutil
import time

ee.Initialize(project='ee-hadat-461702-p4')

# =============================================================================
# FUNCTIONS
# =============================================================================

def apply_scale_factors(image):
    """
    Applies scaling factors to Landsat Collection 2 Level 2 data to derive
    surface temperature in Kelvin.
    """
    # Scale optical bands
    # optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    
    # Scale thermal band (Surface Temperature), results in Kelvin
    thermal_band = image.select('ST_B10').multiply(0.00341802).add(149.0)
    
    # Rename the band to 'LST'
    lst_kelvin = thermal_band.rename('LST')
    
    return image.addBands(lst_kelvin, None, True)

def cloud_mask_landsat(image):
    """
    Masks clouds and cloud shadows in Landsat images using the QA_PIXEL band.
    A more aggressive approach is used here, also masking dilated clouds to reduce
    the impact of cloud edges and haze.
    """
    qa = image.select('QA_PIXEL')
    # Bits 1 (Dilated Cloud), 3 (Cloud), and 4 (Cloud Shadow) are used for masking.
    dilated_cloud = 1 << 1
    cloud = 1 << 3
    cloud_shadow = 1 << 4
    mask = (qa.bitwiseAnd(dilated_cloud).eq(0)
              .And(qa.bitwiseAnd(cloud).eq(0))
              .And(qa.bitwiseAnd(cloud_shadow).eq(0)))
    return image.updateMask(mask)

def get_landsat_collection(start_date, end_date, roi):
    """
    Loads and merges Landsat 8 and 9 collections, applies scaling factors and cloud masking.
    """
    # Load Landsat 9 collection
    l9_collection = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
                         .filterBounds(roi)
                         .filterDate(start_date, end_date))
    
    # Load Landsat 8 collection
    l8_collection = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
                         .filterBounds(roi)
                         .filterDate(start_date, end_date))

    # Merge the two collections
    landsat_collection = l9_collection.merge(l8_collection)
    
    # Apply scaling and cloud masking
    processed_collection = (landsat_collection
                            .map(apply_scale_factors)
                            .map(cloud_mask_landsat))

    return processed_collection

def download_images(image_collection, big_folder, roi, roi_name, folder_name):
    """
    Downloads each LST image from the collection as a ZIP file, extracts the GeoTIFF,
    and moves it to the destination folder. Skips download if file already exists.
    File names are based on the image's system:index.
    """
    image_list = image_collection.toList(image_collection.size())
    size = image_collection.size().getInfo()
    out_folder = os.path.join(big_folder, roi_name, folder_name)
    if not os.path.exists(out_folder):
        os.makedirs(out_folder)

    print(f"Found {size} images to process for download.")

    for i in range(size):
        image = ee.Image(image_list.get(i))
        # Ensure the image has an LST band before downloading.
        if image.bandNames().contains('LST').getInfo():
            image_id = image.get('system:index').getInfo()
            
            final_tif_path = os.path.join(out_folder, f"{image_id}_LST.tif")

            if os.path.exists(final_tif_path):
                print(f"Skipping download for {image_id}, file already exists.")
                continue

            params = {
                'scale': 30,  # Landsat LST is resampled to 30m
                'region': roi,
                'fileFormat': 'ZIP',
                'maxPixels': 1e13
            }
            download_url = image.select('LST').getDownloadURL(params)
            print(f'Downloading LST for image: {image_id}')
            
            # Create a temporary folder for the download.
            temp_dir = tempfile.mkdtemp()
            zip_path = os.path.join(temp_dir, f"{image_id}.zip")
            
            try:
                response = requests.get(download_url, timeout=120)
                response.raise_for_status() 
            except requests.exceptions.RequestException as e:
                print(f"Error fetching URL for {image_id}: {e}")
                shutil.rmtree(temp_dir)
                continue

            with open(zip_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded ZIP file: {zip_path}")
            
            # Unzip and move the GeoTIFF file.
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                tif_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.tif')]
                if tif_files:
                    tif_file = tif_files[0]
                    shutil.move(os.path.join(temp_dir, tif_file), final_tif_path)
                    print(f"Moved GeoTIFF to {final_tif_path}")
                else:
                    print(f"No TIFF file found in ZIP for image: {image_id}")
            except Exception as e:
                print(f"Error processing file for {image_id}: {e}")
            finally:
                shutil.rmtree(temp_dir)
            
            time.sleep(0.5)  # Pause to avoid overwhelming the server.

def main_ls2prd_lst(start_date, end_date, roi, roi_name, big_folder):
    """
    Main function to orchestrate the LST data retrieval and processing workflow.
    Downloads all available images individually without creating mosaics or composites.
    """
    # Define time period.
    s_date = ee.Date(start_date)
    e_date = ee.Date(end_date)

    # 1. Load Landsat 9 collection and apply scaling/masking.
    landsat_collection = get_landsat_collection(s_date, e_date, roi)
    print('Landsat 8 & 9 Collection loaded and pre-processed.')
    
    # 2. Download all available LST images individually.
    download_folder_name = f'{roi_name.split("_")[0]}_lst_images'
    download_images(landsat_collection, big_folder, roi, roi_name, download_folder_name)

# =============================================================================
# EXAMPLE USAGE
# =============================================================================
# if __name__ == '__main__':
#     # Define parameters
#     # --- IMPORTANT: Define your own region of interest (ROI) ---
#     # Example ROI (San Francisco)
#     roi_example = ee.Geometry.Polygon(
#         [[[-122.51, 37.70],
#           [-122.51, 37.81],
#           [-122.38, 37.81],
#           [-122.38, 37.70],
#           [-122.51, 37.70]]])

#     start_date_example = '2022-01-01'
#     end_date_example = '2022-03-31'
#     roi_name_example = 'SF_example'
#     big_folder_example = './LST_data' # Downloads to a subfolder in your project

#     # Create the main data folder if it doesn't exist
#     if not os.path.exists(big_folder_example):
#         os.makedirs(big_folder_example)

#     # Run the main function
#     main_ls2prd_lst(start_date_example, end_date_example, roi_example, roi_name_example, big_folder_example)
