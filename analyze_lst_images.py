import os
import rasterio
import numpy as np
from tqdm import tqdm

def get_image_mean(image_path: str) -> tuple[str, float]:
    """
    Calculates the mean value of a raster image, ignoring NaN values.

    Args:
        image_path (str): The full path to the raster image file.

    Returns:
        tuple[str, float]: A tuple containing the filename and its mean value.
                           Returns (filename, None) if the file cannot be processed.
    """
    filename = os.path.basename(image_path)
    try:
        with rasterio.open(image_path) as src:
            # Read the first band
            image_data = src.read(1).astype(np.float32)
            
            # Get the nodata value from the file's metadata
            nodata_value = src.nodata
            
            # If a nodata value is specified, replace it with NaN
            if nodata_value is not None:
                image_data[image_data == nodata_value] = np.nan
            
            # Calculate the mean, ignoring any NaN values
            if np.all(np.isnan(image_data)):
                mean_value = np.nan # Return NaN if the whole image is NaN
            else:
                mean_value = np.nanmean(image_data)
                
            return (filename, mean_value)
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return (filename, None)

def analyze_lst_folder(folder_path: str):
    """
    Analyzes all LST images in a folder to find the ones with the highest
    and smallest mean values.

    Args:
        folder_path (str): The path to the folder containing LST .tif images.
    """
    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found at '{folder_path}'")
        return

    # Find all .tif files in the specified directory
    tif_files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.tif', '.tiff'))]
    
    if not tif_files:
        print(f"No .tif or .tiff images found in '{folder_path}'")
        return
        
    print(f"Found {len(tif_files)} images. Analyzing mean values...")

    # Calculate the mean for each image
    image_means = []
    for filename in tqdm(tif_files, desc="Calculating image means"):
        full_path = os.path.join(folder_path, filename)
        _, mean_val = get_image_mean(full_path)
        if mean_val is not None and not np.isnan(mean_val):
            image_means.append((filename, mean_val))

    if not image_means:
        print("Could not calculate mean values for any of the images.")
        return

    # Sort the images by their mean value
    # Sort in descending order to easily get highest and lowest
    image_means.sort(key=lambda x: x[1], reverse=True)

    # --- Display the results ---
    print("\n" + "="*50)
    print("ANALYSIS COMPLETE")
    print("="*50)

    # Get the top 10 highest mean images
    top_10_highest = image_means[:10]
    print("\n--- Top 10 Images with HIGHEST Mean LST ---")
    if not top_10_highest:
        print("No images to display.")
    else:
        for i, (filename, mean_val) in enumerate(top_10_highest):
            print(f"{i+1:2d}. {filename}: {mean_val:.4f}")

    # Get the top 10 smallest mean images (from the end of the sorted list)
    # The list is already sorted descending, so the smallest are at the end.
    top_10_smallest = image_means[-10:]
    # We reverse this small list just for clean display (smallest to largest)
    top_10_smallest.reverse() 
    print("\n--- Top 10 Images with SMALLEST Mean LST ---")
    if not top_10_smallest:
        print("No images to display.")
    else:
        for i, (filename, mean_val) in enumerate(top_10_smallest):
            print(f"{i+1:2d}. {filename}: {mean_val:.4f}")

if __name__ == '__main__':
    # --- USER: Set the path to your folder of LST images here ---
    # Using a sample path from your project structure.
    # Please verify this is the correct folder you want to analyze.
    lst_images_folder_path = "/mnt/hdd12tb/code/nhatvm/DELAG/DELAG_LST/download_data_v3/BinhNguyen_KienXuong_ThaiBinh/lst"
    
    analyze_lst_folder(lst_images_folder_path) 