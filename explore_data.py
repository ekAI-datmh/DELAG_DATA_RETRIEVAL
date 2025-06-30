import geopandas as gpd
import rasterio
import numpy as np
import matplotlib.pyplot as plt

def analyze_shapefile(filepath, name):
    """
    Reads and analyzes a shapefile.

    Args:
        filepath (str): Path to the shapefile.
        name (str): Name of the dataset for printing.
    """
    print(f"--- Analyzing Shapefile: {name} ---")
    try:
        gdf = gpd.read_file(filepath)
        print(f"Successfully read {filepath}")
        print("Shapefile Info:")
        print(f"  - CRS: {gdf.crs}")
        print(f"  - Bounding box: {gdf.total_bounds}")
        print(f"  - Number of features: {len(gdf)}")
        print("  - First 5 rows:")
        print(gdf.head())
        print("\n")
        return gdf
    except Exception as e:
        print(f"Could not read or analyze {filepath}. Error: {e}")
        return None

def analyze_raster(filepath, name="Land Cover TIF"):
    """
    Reads and analyzes a raster file.

    Args:
        filepath (str): Path to the TIF file.
        name (str): Name of the dataset for printing.
    """
    print(f"--- Analyzing Raster: {name} ---")
    try:
        with rasterio.open(filepath) as src:
            print(f"Successfully read {filepath}")
            print("Raster Info:")
            print(f"  - CRS: {src.crs}")
            print(f"  - Bounding box: {src.bounds}")
            print(f"  - Dimensions (height, width): ({src.height}, {src.width})")
            print(f"  - Number of bands: {src.count}")
            print(f"  - Data type: {src.dtypes[0]}")

            if src.count == 1:
                print("  - Analyzing single band...")
                band1 = src.read(1)
                unique_values, counts = np.unique(band1, return_counts=True)
                print(f"  - Unique values in band 1: {unique_values}")
                print(f"  - Min value: {np.min(band1)}")
                print(f"  - Max value: {np.max(band1)}")

                # Plot histogram of land cover labels
                plt.figure(figsize=(10, 6))
                plt.bar(unique_values, counts, tick_label=unique_values)
                plt.xlabel("Land Cover Label")
                plt.ylabel("Pixel Count")
                plt.title("Distribution of Land Cover Labels")
                plt.grid(axis='y', alpha=0.75)
                # plt.show()
                # Instead of showing, save it to a file
                output_filename = 'land_cover_distribution.png'
                plt.savefig(output_filename)
                print(f"\nSaved land cover distribution plot to {output_filename}")


        print("\n")
        return src
    except Exception as e:
        print(f"Could not read or analyze {filepath}. Error: {e}")
        return None

if __name__ == '__main__':
    # --- PLEASE PROVIDE THE PATHS TO YOUR FILES HERE ---
    # Example for grid shapefile. You might find it in the 'shapefile/' directory.
    grid_shp_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/popGrids3x3.shp" 
    
    # Example for region shapefile.
    region_shp_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/vnm_admbnda_7vungsinhthai_MERGE.shp"
    
    # Example for land cover GeoTIFF file.
    land_cover_tif_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/VN_JAXA.tif"
    # ----------------------------------------------------

    print("Starting data analysis...")

    # Analyze Grid Shapefile
    grid_gdf = analyze_shapefile(grid_shp_path, "Grid")

    # Analyze Region Shapefile
    region_gdf = analyze_shapefile(region_shp_path, "Region")

    # Analyze Land Cover TIFF
    # land_cover_raster = analyze_raster(land_cover_tif_path, "Land Cover")

    if grid_gdf is not None and region_gdf is not None:
        print("--- Comparing Shapefile CRS ---")
        if grid_gdf.crs == region_gdf.crs:
            print("Grid and Region shapefiles have the same CRS.")
        else:
            print("Warning: Grid and Region shapefiles have different CRS.")
            print(f"  - Grid CRS: {grid_gdf.crs}")
            print(f"  - Region CRS: {region_gdf.crs}")

    # if grid_gdf is not None and land_cover_raster is not None:
    #     print("\n--- Comparing Grid Shapefile and Raster CRS ---")
    #     if grid_gdf.crs == land_cover_raster.crs:
    #         print("Grid shapefile and Raster have the same CRS.")
    #     else:
    #         print("Warning: Grid shapefile and Raster have different CRS.")
    #         print(f"  - Grid CRS: {grid_gdf.crs}")
    #         print(f"  - Raster CRS: {land_cover_raster.crs}")


    print("\nData analysis complete.")
    print("Please check the generated 'land_cover_distribution.png' for the land cover distribution.") 