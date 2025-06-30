import geopandas as gpd
import rasterio
import numpy as np
import pandas as pd
from rasterio.features import rasterize
from shapely.geometry import Point
import matplotlib.pyplot as plt
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
from functools import partial
from tqdm import tqdm

def process_single_grid(grid_data, landcover_tif_path, target_landcover_classes, min_classes_required, min_coverage_threshold):
    """
    Process a single grid cell for land cover analysis.
    This function is designed to be called in parallel.
    
    Args:
        grid_data: Tuple of (idx, grid_row) from iterrows()
        landcover_tif_path: Path to land cover raster
        target_landcover_classes: List of target land cover classes
        min_classes_required: Minimum number of target classes required
        min_coverage_threshold: Minimum coverage threshold for each class
    
    Returns:
        dict or None: Grid result dictionary if criteria met, None otherwise
    """
    idx, grid = grid_data
    
    try:
        # Load land cover raster (each process loads its own copy)
        # Use a context manager to ensure proper cleanup
        with rasterio.open(landcover_tif_path, 'r') as src:
            landcover_data = src.read(1)
            transform = src.transform
            
            # Get grid geometry
            grid_geom = grid.geometry
            
            # Create a mask for this grid cell
            mask = rasterize(
                [grid_geom],
                out_shape=landcover_data.shape,
                transform=transform,
                fill=0,
                default_value=1,
                dtype='uint8'
            )
            
            # Extract land cover values within this grid
            grid_landcover = landcover_data[mask == 1]
            
            if len(grid_landcover) == 0:
                return None
                
            # Count land cover classes
            class_counts = Counter(grid_landcover)
            total_pixels = len(grid_landcover)
            
            # Calculate coverage percentages for target classes
            target_class_coverage = {}
            classes_meeting_threshold = 0
            
            for lc_class in target_landcover_classes:
                coverage = class_counts.get(lc_class, 0) / total_pixels
                target_class_coverage[f'coverage_class_{lc_class}'] = coverage
                
                if coverage >= min_coverage_threshold:
                    classes_meeting_threshold += 1
            
            # Check if this grid meets the criteria
            if classes_meeting_threshold >= min_classes_required:
                # Get grid center coordinates
                centroid = grid_geom.centroid
                
                # Calculate grid dimensions (assuming rectangular grid)
                bounds = grid_geom.bounds
                width_deg = bounds[2] - bounds[0]  # max_x - min_x
                height_deg = bounds[3] - bounds[1]  # max_y - min_y
                
                result = {
                    'grid_id': grid.get('id_grid', f'grid_{idx}'),
                    'center_longitude': centroid.x,
                    'center_latitude': centroid.y,
                    'area_grid': grid.get('area_grid', grid_geom.area),
                    'width_degrees': width_deg,
                    'height_degrees': height_deg,
                    'classes_meeting_threshold': classes_meeting_threshold,
                    'total_pixels': total_pixels,
                    'all_classes_found': list(class_counts.keys()),  # For debugging
                    **target_class_coverage
                }
                
                # Add all land cover class percentages for reference
                for lc_class in range(13):  # 0-12 land cover classes
                    if lc_class not in target_landcover_classes:
                        coverage = class_counts.get(lc_class, 0) / total_pixels
                        result[f'coverage_class_{lc_class}'] = coverage
                
                return result
            
            return None
            
    except Exception as e:
        print(f"Error processing grid {idx}: {e}")
        return None

def filter_tay_nguyen_grids(grid_shp_path, region_shp_path, landcover_tif_path, 
                           target_landcover_classes=[1, 2, 4, 5, 7, 8], 
                           min_classes_required=3, 
                           min_coverage_threshold=0.1,
                           num_sampled_grids=None,
                           test_mode=False,
                           max_test_grids=10,
                           n_workers=None):
    """
    Filter grid cells in Tay Nguyen region that have significant coverage 
    in at least min_classes_required of the target land cover classes.
    
    Args:
        grid_shp_path (str): Path to grid shapefile
        region_shp_path (str): Path to region shapefile
        landcover_tif_path (str): Path to land cover raster
        target_landcover_classes (list): Land cover classes to check [1, 2, 4, 5, 7, 8]
        min_classes_required (int): Minimum number of target classes that must be present (default: 3)
        min_coverage_threshold (float): Minimum coverage percentage for a class to be considered present (default: 0.1 = 10%)
        num_sampled_grids (int, optional): If set, sample this many grids, prioritizing those with more land cover classes.
        test_mode (bool): If True, limit processing to max_test_grids for testing (default: False)
        max_test_grids (int): Maximum number of grids to process in test mode (default: 10)
        n_workers (int): Number of parallel workers (default: None = auto-detect CPU cores)
    
    Returns:
        gpd.GeoDataFrame: Filtered grid cells with land cover statistics
    """
    
    print("📂 Loading data...")
    
    # Load shapefiles with progress indication
    with tqdm(total=2, desc="Loading shapefiles", unit="file", colour='blue') as pbar:
        grid_gdf = gpd.read_file(grid_shp_path)
        pbar.set_postfix_str(f"Grid: {len(grid_gdf)} cells")
        pbar.update(1)
        
        region_gdf = gpd.read_file(region_shp_path)
        pbar.set_postfix_str(f"Regions: {len(region_gdf)} features")
        pbar.update(1)
    
    print(f"✅ Loaded {len(grid_gdf)} grid cells and {len(region_gdf)} regions")
    
    # Ensure both datasets have the same CRS
    if grid_gdf.crs != region_gdf.crs:
        print(f"🔄 CRS mismatch detected. Converting grid CRS...")
        print(f"   From: {grid_gdf.crs}")
        print(f"   To: {region_gdf.crs}")
        
        with tqdm(total=1, desc="Converting CRS", unit="dataset", colour='yellow') as pbar:
            grid_gdf = grid_gdf.to_crs(region_gdf.crs)
            pbar.update(1)
        
        print("✅ CRS conversion completed.")
    
    # Filter for Tay Nguyen region
    tay_nguyen = region_gdf[region_gdf['ADM_VST_EN'].str.contains('Tay Nguyen', case=False, na=False)]
    
    if len(tay_nguyen) == 0:
        print("Warning: No 'Tay Nguyen' region found. Available regions:")
        print(region_gdf['ADM_VST_EN'].tolist())
        return None
    
    print(f"Found Tay Nguyen region with {len(tay_nguyen)} features")
    
    # Spatial intersection to find grids in Tay Nguyen
    print("🗺️  Finding grids in Tay Nguyen region...")
    
    with tqdm(total=1, desc="Spatial intersection", unit="operation", colour='cyan') as pbar:
        grids_in_tay_nguyen = gpd.overlay(grid_gdf, tay_nguyen, how='intersection')
        pbar.set_postfix_str(f"Found {len(grids_in_tay_nguyen)} grids")
        pbar.update(1)
    
    print(f"✅ Found {len(grids_in_tay_nguyen)} grids in Tay Nguyen region")
    
    if len(grids_in_tay_nguyen) == 0:
        print("No grids found in Tay Nguyen region!")
        return None
    
    # Apply test mode limitation if enabled
    if test_mode:
        original_count = len(grids_in_tay_nguyen)
        # Sample randomly instead of taking first N to get more diverse grids
        grids_in_tay_nguyen = grids_in_tay_nguyen.sample(n=min(max_test_grids, len(grids_in_tay_nguyen)), random_state=42)
        print(f"🧪 TEST MODE: Processing {len(grids_in_tay_nguyen)} randomly sampled grids out of {original_count} total grids")
    
    # Determine number of workers - limit to prevent system overload
    if n_workers is None:
        # Limit to max 8 workers to prevent memory issues with large raster files
        n_workers = min(8, multiprocessing.cpu_count(), len(grids_in_tay_nguyen))
    else:
        n_workers = min(n_workers, 8)  # Hard limit to prevent system overload
    
    print(f"🚀 Starting parallel processing with {n_workers} workers...")
    print(f"Analyzing land cover for {len(grids_in_tay_nguyen)} grids...")
    
    # Prepare grid data for parallel processing
    grid_data_list = list(grids_in_tay_nguyen.iterrows())
    
    # Create partial function with fixed parameters
    process_func = partial(
        process_single_grid,
        landcover_tif_path=landcover_tif_path,
        target_landcover_classes=target_landcover_classes,
        min_classes_required=min_classes_required,
        min_coverage_threshold=min_coverage_threshold
    )
    
    # Process grids in parallel
    results = []
    all_classes_found = set()
    qualifying_grids = 0
    
    # Use ThreadPoolExecutor for better memory sharing with large raster files
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        # Submit all jobs
        future_to_grid = {executor.submit(process_func, grid_data): grid_data[0] for grid_data in grid_data_list}
        
        # Create progress bar
        with tqdm(total=len(grid_data_list), desc="🔍 Analyzing grids", 
                  unit="grid", ncols=100, colour='green') as pbar:
            
            # Collect results as they complete
            for future in as_completed(future_to_grid):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                        qualifying_grids += 1
                        # Collect debug info
                        if 'all_classes_found' in result:
                            all_classes_found.update(result['all_classes_found'])
                            # Remove debug field from final result
                            del result['all_classes_found']
                        
                        # Update progress bar description with current qualifying count
                        pbar.set_postfix({
                            'qualifying': qualifying_grids,
                            'workers': n_workers
                        })
                        
                except Exception as e:
                    grid_idx = future_to_grid[future]
                    tqdm.write(f"⚠️  Error processing grid {grid_idx}: {e}")
                
                # Update progress bar
                pbar.update(1)
    
    print(f"✅ Parallel processing completed!")
    print(f"Found {len(results)} grids meeting the criteria")
    
    # Debug information
    print(f"🔍 Debug Info:")
    print(f"   - Processed {len(grid_data_list)} grids")
    print(f"   - Qualifying grids found: {len(results)}")
    print(f"   - Land cover classes found: {sorted(all_classes_found)}")
    print(f"   - Target classes: {target_landcover_classes}")
    print(f"   - Criteria: {min_classes_required} classes with ≥{min_coverage_threshold*100}% coverage")
    
    if len(results) == 0:
        print("❌ No grids found meeting the land cover criteria!")
        print("💡 Suggestions:")
        print("   - Try reducing min_classes_required or min_coverage_threshold")
        print("   - Check if target land cover classes exist in this region")
        return None
    
    # Create GeoDataFrame with results
    results_df = pd.DataFrame(results)
    
    # Create geometry from center coordinates
    geometry = [Point(row['center_longitude'], row['center_latitude']) for _, row in results_df.iterrows()]
    results_gdf = gpd.GeoDataFrame(results_df, geometry=geometry, crs='EPSG:4326')
    
    # If sampling is requested, select the top N grids based on land cover diversity
    if num_sampled_grids is not None and len(results_gdf) > num_sampled_grids:
        print(f"\nSampling {num_sampled_grids} grids from {len(results_gdf)} qualifying grids...")
        print("Prioritizing grids with the highest number of target land cover classes...")

        # Sort by the number of classes meeting the threshold, descending
        sorted_gdf = results_gdf.sort_values(by='classes_meeting_threshold', ascending=False)
        
        sampled_indices = []
        # Group by the number of classes and iterate from most diverse to least
        for _, group in sorted_gdf.groupby('classes_meeting_threshold', sort=False):
            needed = num_sampled_grids - len(sampled_indices)
            if needed <= 0:
                break
            
            if len(group) <= needed:
                # Take all grids in this group if it fits
                sampled_indices.extend(group.index.tolist())
            else:
                # Otherwise, randomly sample from this group to fill the remainder
                sampled_indices.extend(group.sample(n=needed, random_state=42).index.tolist())

        results_gdf = results_gdf.loc[sampled_indices].reset_index(drop=True)
        print(f"✅ Done. Selected {len(results_gdf)} grids for the final dataset.")
    
    return results_gdf

def visualize_results(results_gdf, region_gdf, output_path='tay_nguyen_filtered_grids.png'):
    """
    Create a visualization of the filtered grids
    """
    if results_gdf is None:
        return
        
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))
    
    # Plot Tay Nguyen region
    tay_nguyen = region_gdf[region_gdf['ADM_VST_EN'].str.contains('Tay Nguyen', case=False, na=False)]
    tay_nguyen.plot(ax=ax, color='lightblue', alpha=0.5, edgecolor='blue', linewidth=2)
    
    # Plot filtered grid centers
    results_gdf.plot(ax=ax, color='red', markersize=5, alpha=0.7)
    
    ax.set_title(f'Filtered Grid Centers in Tay Nguyen Region\n({len(results_gdf)} grids selected)', fontsize=14)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to {output_path}")

def main():
    # File paths
    grid_shp_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/SoDo10K.shp"
    region_shp_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/vnm_admbnda_7vungsinhthai_MERGE.shp"
    landcover_tif_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/shapefile/VN_JAXA.tif"
    
    # Configuration parameters
    target_classes = [1, 2, 4, 5, 7, 8]  # Target land cover classes
    min_classes = 2  # At least 2 of the 6 target classes must be present
    min_coverage = 0.1 # At least 10% coverage for each class
    NUM_SAMPLED_GRIDS = 30 # Select 30 grids, prioritizing diversity
    
    # 🧪 TEST MODE CONFIGURATION
    TEST_MODE = False  # Set to False for full processing
    MAX_TEST_GRIDS = 50  # Number of grids to process in test mode (increased for better chance of finding qualifying grids)
    
    # 🚀 PARALLEL PROCESSING CONFIGURATION
    N_WORKERS = 4  # Limited to 4 workers to prevent system overload with large raster files
    
    print("=== Filtering Tay Nguyen Grids ===")
    print(f"Target land cover classes: {target_classes}")
    print(f"Minimum classes required: {min_classes} out of {len(target_classes)}")
    print(f"Minimum coverage threshold: {min_coverage*100}%")
    if NUM_SAMPLED_GRIDS:
        print(f"Grid sampling enabled: Will select the best {NUM_SAMPLED_GRIDS} grids.")
    
    if TEST_MODE:
        print(f"🧪 TEST MODE ENABLED: Will process maximum {MAX_TEST_GRIDS} grids")
        print("   Set TEST_MODE = False in the code for full processing")
    else:
        print("🚀 FULL MODE: Will process all qualifying grids")
    
    # Show parallel processing info
    cpu_count = multiprocessing.cpu_count()
    workers_to_use = N_WORKERS if N_WORKERS is not None else cpu_count
    print(f"🚀 PARALLEL PROCESSING: Using {workers_to_use} workers (CPU cores available: {cpu_count})")
    print()
    
    # Filter grids
    filtered_grids = filter_tay_nguyen_grids(
        grid_shp_path, 
        region_shp_path, 
        landcover_tif_path,
        target_landcover_classes=target_classes,
        min_classes_required=min_classes,
        min_coverage_threshold=min_coverage,
        num_sampled_grids=NUM_SAMPLED_GRIDS,
        test_mode=TEST_MODE,
        max_test_grids=MAX_TEST_GRIDS,
        n_workers=N_WORKERS
    )
    
    if filtered_grids is not None:
        print("\n=== Results Summary ===")
        print(f"Total grids selected: {len(filtered_grids)}")
        print(f"Average classes meeting threshold: {filtered_grids['classes_meeting_threshold'].mean():.2f}")
        print("\nFirst 5 selected grids:")
        print(filtered_grids[['grid_id', 'center_longitude', 'center_latitude', 'classes_meeting_threshold']].head())
        
        # Analyze and report on land cover presence in the final dataset
        print("\n=== Land Cover Analysis of Selected Grids ===")
        present_labels = []
        for lc_class in target_classes:
            coverage_col = f'coverage_class_{lc_class}'
            # Check if any grid in the final set has this class with significant coverage
            if (filtered_grids[coverage_col] >= min_coverage).any():
                present_labels.append(lc_class)
        
        print(f"Found {len(present_labels)} out of {len(target_classes)} target land cover labels in the final {len(filtered_grids)} grids.")
        print(f"Present labels: {sorted(present_labels)}")

        # Save results with progress indication
        print("\n💾 Saving results...")
        
        with tqdm(total=3, desc="Saving files", unit="file", colour='magenta') as pbar:
            # Save CSV
            output_csv = 'tay_nguyen_filtered_grids.csv'
            filtered_grids.drop('geometry', axis=1).to_csv(output_csv, index=False)
            pbar.set_postfix_str(f"CSV: {output_csv}")
            pbar.update(1)
            
            # Save shapefile
            output_shp = 'tay_nguyen_filtered_grids.shp'
            filtered_grids.to_file(output_shp)
            pbar.set_postfix_str(f"SHP: {output_shp}")
            pbar.update(1)
            
            # Create visualization
            region_gdf = gpd.read_file(region_shp_path)
            visualize_results(filtered_grids, region_gdf)
            pbar.set_postfix_str("Visualization created")
            pbar.update(1)
        
        print(f"✅ Results saved to {output_csv}")
        print(f"✅ Shapefile saved to {output_shp}")
        
        # Show land cover statistics
        print("\n=== Land Cover Statistics ===")
        target_cols = [f'coverage_class_{c}' for c in target_classes]
        print("Average coverage for target classes:")
        for col in target_cols:
            avg_coverage = filtered_grids[col].mean()
            print(f"  Class {col.split('_')[-1]}: {avg_coverage*100:.2f}%")

if __name__ == "__main__":
    main() 