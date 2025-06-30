import rasterio
import os
import re
from datetime import datetime, timedelta
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

def extract_date_from_filename(filename):
    """
    Extract date from filename using common patterns.
    
    Args:
        filename (str): Filename to extract date from
        
    Returns:
        datetime or None: Extracted date or None if not found
    """
    # Common date patterns in filenames
    patterns = [
        r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
        r'(\d{4}_\d{2}_\d{2})',  # YYYY_MM_DD
        r'(\d{8})',              # YYYYMMDD
        r'(\d{4}\d{2}\d{2})',    # YYYYMMDD (no separators)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            date_str = match.group(1)
            try:
                if '-' in date_str:
                    return datetime.strptime(date_str, '%Y-%m-%d')
                elif '_' in date_str:
                    return datetime.strptime(date_str, '%Y_%m_%d')
                elif len(date_str) == 8:
                    return datetime.strptime(date_str, '%Y%m%d')
            except ValueError:
                continue
    return None

def calculate_day_of_year(date_obj):
    """
    Calculate day of year from datetime object.
    
    Args:
        date_obj (datetime): Date object
        
    Returns:
        int: Day of year (1-366)
    """
    if date_obj:
        return date_obj.timetuple().tm_yday
    return None

def check_tif_metadata(tif_path):
    """
    Check and display metadata of a GeoTIFF file including date and time information.
    
    Args:
        tif_path (str): Path to the GeoTIFF file
    """
    if not os.path.exists(tif_path):
        print(f"Error: File not found - {tif_path}")
        return
    
    try:
        filename = os.path.basename(tif_path)
        
        # Extract date information
        image_date = extract_date_from_filename(filename)
        
        with rasterio.open(tif_path) as dataset:
            print(f"\n=== Metadata for: {os.path.basename(tif_path)} ===")
            print(f"File path: {tif_path}")
            
            # Date and time information
            if image_date:
                day_of_year = calculate_day_of_year(image_date)
                print(f"Image date: {image_date.strftime('%Y-%m-%d')}")
                print(f"Day of year: {day_of_year}")
                print(f"Year: {image_date.year}")
                print(f"Month: {image_date.month}")
                print(f"Day: {image_date.day}")
                print(f"Week of year: {image_date.isocalendar()[1]}")
            else:
                print(f"Image date: Not found in filename")
            
            # File creation/modification time
            file_stats = os.stat(tif_path)
            creation_time = datetime.fromtimestamp(file_stats.st_ctime)
            modification_time = datetime.fromtimestamp(file_stats.st_mtime)
            print(f"File created: {creation_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"File modified: {modification_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            print(f"Driver: {dataset.driver}")
            print(f"Width: {dataset.width} pixels")
            print(f"Height: {dataset.height} pixels")
            print(f"Number of bands: {dataset.count}")
            print(f"Data type: {dataset.dtypes}")
            print(f"CRS: {dataset.crs}")
            print(f"Transform: {dataset.transform}")
            print(f"Bounds (native CRS): {dataset.bounds}")
            
            # Transform bounds to EPSG:4326 if different
            if dataset.crs and dataset.crs.to_string() != 'EPSG:4326':
                bounds_4326 = transform_bounds(dataset.crs, 'EPSG:4326', *dataset.bounds)
                print(f"Bounds (EPSG:4326): {bounds_4326}")
            
            # Band information
            for i in range(1, dataset.count + 1):
                band = dataset.read(i)
                print(f"Band {i}:")
                print(f"  - Min value: {band.min()}")
                print(f"  - Max value: {band.max()}")
                print(f"  - Mean value: {band.mean():.4f}")
                print(f"  - NoData value: {dataset.nodata}")
            
            # Additional metadata
            if dataset.tags():
                print(f"Tags: {dataset.tags()}")
            
            # Pixel size
            pixel_size_x = abs(dataset.transform[0])
            pixel_size_y = abs(dataset.transform[4])
            print(f"Pixel size: {pixel_size_x} x {pixel_size_y} (units of CRS)")
            
    except Exception as e:
        print(f"Error reading TIF file {tif_path}: {e}")

def check_folder_tifs(folder_path):
    """
    Check metadata for all TIF files in a folder with time analysis.
    
    Args:
        folder_path (str): Path to folder containing TIF files
    """
    if not os.path.exists(folder_path):
        print(f"Error: Folder not found - {folder_path}")
        return
    
    tif_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.tif')]
    
    if not tif_files:
        print(f"No TIF files found in {folder_path}")
        return
    
    print(f"Found {len(tif_files)} TIF files in {folder_path}")
    
    # Extract dates from all files for time analysis
    file_dates = []
    for tif_file in sorted(tif_files):
        image_date = extract_date_from_filename(tif_file)
        if image_date:
            file_dates.append((tif_file, image_date))
    
    # Sort by date
    file_dates.sort(key=lambda x: x[1])
    
    # Time analysis summary
    if file_dates:
        print(f"\n=== TIME ANALYSIS SUMMARY ===")
        print(f"Date range: {file_dates[0][1].strftime('%Y-%m-%d')} to {file_dates[-1][1].strftime('%Y-%m-%d')}")
        total_days = (file_dates[-1][1] - file_dates[0][1]).days
        print(f"Total time span: {total_days} days")
        print(f"Number of images with dates: {len(file_dates)}")
        
        if len(file_dates) > 1:
            # Calculate intervals between consecutive images
            intervals = []
            for i in range(1, len(file_dates)):
                interval = (file_dates[i][1] - file_dates[i-1][1]).days
                intervals.append(interval)
            
            print(f"Average interval: {sum(intervals)/len(intervals):.1f} days")
            print(f"Min interval: {min(intervals)} days")
            print(f"Max interval: {max(intervals)} days")
            
            # Check for gaps (intervals > 30 days)
            large_gaps = [i for i in intervals if i > 30]
            if large_gaps:
                print(f"Large gaps (>30 days): {len(large_gaps)} gaps")
                print(f"Largest gap: {max(large_gaps)} days")
        
        # Day of year analysis
        days_of_year = [calculate_day_of_year(date) for _, date in file_dates]
        print(f"Day of year range: {min(days_of_year)} to {max(days_of_year)}")
        
        # Year coverage
        years = list(set([date.year for _, date in file_dates]))
        years.sort()
        print(f"Years covered: {years}")
        print(f"Images per year:")
        for year in years:
            year_count = len([date for _, date in file_dates if date.year == year])
            print(f"  {year}: {year_count} images")
    
    print(f"\n=== INDIVIDUAL FILE METADATA ===")
    for tif_file in sorted(tif_files):
        tif_path = os.path.join(folder_path, tif_file)
        check_tif_metadata(tif_path)

# Example usage
if __name__ == "__main__":
    # Check a single TIF file
    tif_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/sample_data/BinhNguyen_KienXuong_ThaiBinh/lst/lst16days_2022-12-20.tif"
    check_tif_metadata(tif_path)
    
    # # Check all TIF files in a folder with time analysis
    # print("\n" + "="*50)
    # folder_path = "/mnt/hdd12tb/code/nhatvm/DELAG_data_retrieval/download_data_v2/KhanhXuan_BuonMaThuot_DakLak/era5"
    # check_folder_tifs(folder_path)
    
    print("\nTIF metadata checker with time analysis ready.")
    print("Functions available:")
    print("- check_tif_metadata(tif_path): Check single TIF file")
    print("- check_folder_tifs(folder_path): Check all TIF files in folder with time analysis")
    print("- extract_date_from_filename(filename): Extract date from filename")
    print("- calculate_day_of_year(date_obj): Calculate day of year from date")
