import os
import glob
import time
from datetime import datetime, timedelta
from pathlib import Path
import rasterio
from rasterio.warp import transform_bounds
import ee
import requests
import shutil
import zipfile

# ================================
# CONFIGURATION
# ================================
TARGET_CRS = 'EPSG:4326'           # Output CRS
EXPORT_SCALE = 27830               # GLDAS native ≈ 0.25° => ~25 km
SKIP_EXISTING = True               # Skip download if file already exists & passes verification
GLDAS_COLLECTION = 'NASA/GLDAS/V021/NOAH/G025/T3H'  # Daily 0.25° product
GLDAS_BANDS = ['SoilTMP0_10cm_inst', 'SoilTMP10_40cm_inst', 'SoilTMP40_100cm_inst', 'SoilTMP100_200cm_inst']    # Surface skin Temp & 2-m Air Temp

# ================================
# Earth Engine init
# ================================
try:
    ee.Initialize(project='ee-hadat-461702-p4')
except Exception:
    print('Authenticating to Earth Engine…')
    ee.Authenticate()
    ee.Initialize(project='ee-hadat-461702-p4')

# ================================
# Helper functions (borrowed from era5_retriever)
# ================================

def get_roi_coords_from_tif(tif_path):
    """Return bbox coords (EPSG:4326) from a sample GeoTIFF."""
    with rasterio.open(tif_path) as ds:
        bounds = ds.bounds
        if ds.crs.to_string() != TARGET_CRS:
            bounds = transform_bounds(ds.crs, TARGET_CRS, *bounds)
    coords = [
        [bounds[0], bounds[1]], [bounds[2], bounds[1]],
        [bounds[2], bounds[3]], [bounds[0], bounds[3]],
        [bounds[0], bounds[1]]
    ]
    return [[float(x), float(y)] for x, y in coords]

def get_dates_from_folder(folder_path):
    """Extract unique YYYY-MM-DD dates from filenames ending in .tif"""
    dates = set()
    for tif in glob.glob(os.path.join(folder_path, '*.tif')):
        base = os.path.basename(tif)
        try:
            d = base.split('_')[-1].replace('.tif', '')
            dates.add(datetime.strptime(d, '%Y-%m-%d'))
        except (ValueError, IndexError):
            continue
    return sorted(dates)

def verify_image(img_path):
    try:
        with rasterio.open(img_path) as src:
            return src.crs is not None and src.width > 0 and src.height > 0
    except Exception:
        return False

def export_ee_image(image, bands, region, out_path):
    tmp_dir = os.path.join(os.path.dirname(out_path), 'tmp_dl')
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        geom = ee.Geometry.Polygon(region, proj=TARGET_CRS, evenOdd=False)
        image = image.clip(geom).select(bands)
        if not image.bandNames().size().getInfo():
            print('  > No bands after clipping; skipping.')
            return
        url = image.getDownloadURL({'scale': EXPORT_SCALE, 'region': region, 'fileFormat': 'GeoTIFF', 'crs': TARGET_CRS})
        zip_path = os.path.join(tmp_dir, 'dl.zip')
        resp = requests.get(url, stream=True, timeout=600)
        resp.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in resp.iter_content(1024*1024):
                f.write(chunk)
        with zipfile.ZipFile(zip_path) as zf:
            tif_files = [f for f in zf.namelist() if f.endswith('.tif')]

            if not tif_files:
                print('  > ZIP had no tif. Skipped.')
                return

            # Extract all tifs
            for tif in tif_files:
                zf.extract(tif, tmp_dir)

            # Order tif files so they correspond to the required band order when possible
            ordered_tifs = []
            for b in bands:
                match = next((t for t in tif_files if b in os.path.basename(t)), None)
                if match:
                    ordered_tifs.append(match)

            if len(ordered_tifs) != len(bands):
                ordered_tifs = tif_files  # Fallback to original order

            # Build multi-band raster
            first_tif_path = os.path.join(tmp_dir, ordered_tifs[0])
            with rasterio.open(first_tif_path) as src0:
                profile = src0.profile
                band_arrays = [src0.read(1)]

            for tif in ordered_tifs[1:]:
                with rasterio.open(os.path.join(tmp_dir, tif)) as src:
                    band_arrays.append(src.read(1))

            profile.update(count=len(band_arrays))

            with rasterio.open(out_path, 'w', **profile) as dst:
                for idx, arr in enumerate(band_arrays, start=1):
                    dst.write(arr, idx)

            print(f'  > Saved multi-band {os.path.basename(out_path)} with {len(band_arrays)} bands')
    except Exception as e:
        print(f'  > Error export {out_path}: {e}')
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# -------------------------------------------------
# Resample to match reference grid (borrowed logic)
# -------------------------------------------------

def resample_to_match_reference(source_path, reference_path):
    """
    Resamples a source GeoTIFF to match the CRS, transform, and dimensions of a
    reference GeoTIFF so images align perfectly.
    """
    try:
        with rasterio.open(reference_path) as ref:
            ref_meta = ref.meta.copy()

        with rasterio.open(source_path) as src:
            if (src.width == ref_meta['width'] and src.height == ref_meta['height'] and src.transform == ref_meta['transform']):
                return  # Already aligned

            ref_meta.update({
                'count': src.count,
                'dtype': src.dtypes[0],
                'nodata': src.nodata
            })

            temp_out = source_path + '.resampled.tif'
            with rasterio.open(temp_out, 'w', **ref_meta) as dst:
                for i in range(1, src.count + 1):
                    rasterio.warp.reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=ref_meta['transform'],
                        dst_crs=ref_meta['crs'],
                        resampling=rasterio.warp.Resampling.bilinear
                    )
            shutil.move(temp_out, source_path)
    except Exception as e:
        print(f"  > Resample error for {os.path.basename(source_path)}: {e}")

# ================================
# Core function
# ================================

def download_gldas_lsts(lst_folder, out_folder):
    """Download daily GLDAS LST-related bands for dates inferred from `lst_folder`."""
    os.makedirs(out_folder, exist_ok=True)
    sample_tif = next(Path(lst_folder).glob('*.tif'), None)
    if not sample_tif:
        print(f'No sample tif in {lst_folder}. Abort GLDAS.')
        return
    roi_coords = get_roi_coords_from_tif(str(sample_tif))
    roi_geom = ee.Geometry.Polygon(roi_coords, proj=TARGET_CRS, evenOdd=False)

    dates = get_dates_from_folder(lst_folder)
    if not dates:
        print(f'No valid dates parsed from {lst_folder}.')
        return
    print(f'GLDAS: Processing {len(dates)} dates for ROI derived from {sample_tif.name}.')

    for date in dates:
        date_str = date.strftime('%Y-%m-%d')
        out_path = os.path.join(out_folder, f'GLDAS21_{date_str}.tif')
        if SKIP_EXISTING and os.path.exists(out_path) and verify_image(out_path):
            resample_to_match_reference(out_path, str(sample_tif))
            print(f'Skip existing verified {os.path.basename(out_path)}')
            continue
        try:
            img = ee.ImageCollection(GLDAS_COLLECTION) \
                    .filterDate(date_str, (date + timedelta(days=1)).strftime('%Y-%m-%d')) \
                    .filterBounds(roi_geom) \
                    .first()
            if img is None:
                print(f'  > No GLDAS image for {date_str}')
                continue
            export_ee_image(img, GLDAS_BANDS, roi_coords, out_path)
            # resample to match reference grid
            if os.path.exists(out_path):
                resample_to_match_reference(out_path, str(sample_tif))
            time.sleep(0.5)
        except ee.EEException as eee:
            print(f'EE error {date_str}: {eee}')
        except Exception as ex:
            print(f'Error {date_str}: {ex}')

# ================================
# CLI
# ================================
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Download GLDAS-2.1 LST bands matching dates in an LST folder.')
    parser.add_argument('--lst_folder', required=True, help='Folder containing LST tif files (dates inferred).')
    parser.add_argument('--out_folder', required=True, help='Folder to save downloaded GLDAS images.')
    args = parser.parse_args()

    download_gldas_lsts(args.lst_folder, args.out_folder) 