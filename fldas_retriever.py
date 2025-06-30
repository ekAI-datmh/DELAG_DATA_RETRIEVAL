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
TARGET_CRS = 'EPSG:4326'
EXPORT_SCALE = 10000  # FLDAS 0.1° ≈ 10 km
SKIP_EXISTING = True
FLDAS_COLLECTION = 'NASA/FLDAS/NOAH01/C/GL/M/V001'  # Monthly 0.1°
FLDAS_BANDS = [
    'SoilTemp00_10cm_tavg',
    'SoilTemp10_40cm_tavg',
    'SoilTemp40_100cm_tavg',
    'SoilTemp100_200cm_tavg'
]

# ================================
# EE init
# ================================
try:
    ee.Initialize(project='ee-hadat-461702-p4')
except Exception:
    ee.Authenticate()
    ee.Initialize(project='ee-hadat-461702-p4')

# ================================
# Utilities
# ================================

def get_roi_coords_from_tif(tif_path):
    with rasterio.open(tif_path) as ds:
        bounds = ds.bounds
        if ds.crs.to_string() != TARGET_CRS:
            bounds = transform_bounds(ds.crs, TARGET_CRS, *bounds)
    return [
        [bounds[0], bounds[1]], [bounds[2], bounds[1]],
        [bounds[2], bounds[3]], [bounds[0], bounds[3]],
        [bounds[0], bounds[1]]
    ]

def get_dates(folder):
    dates = set()
    for tif in glob.glob(os.path.join(folder, '*.tif')):
        base = os.path.basename(tif)
        try:
            dates.add(datetime.strptime(base.split('_')[-1].replace('.tif', ''), '%Y-%m-%d'))
        except (ValueError, IndexError):
            continue
    return sorted(dates)

def verify(img):
    try:
        with rasterio.open(img) as src:
            return src.crs and src.width > 0 and src.height > 0
    except Exception:
        return False

def export_image(img, bands, region, out_path):
    tmp_dir = os.path.join(os.path.dirname(out_path), 'tmp_dl')
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        geom = ee.Geometry.Polygon(region, proj=TARGET_CRS, evenOdd=False)
        img = img.clip(geom).select(bands)
        if not img.bandNames().size().getInfo():
            print('  > No bands after clip. Skip.')
            return
        url = img.getDownloadURL({'scale': EXPORT_SCALE, 'region': region, 'crs': TARGET_CRS, 'fileFormat': 'GeoTIFF'})
        zip_path = os.path.join(tmp_dir, 'dl.zip')
        r = requests.get(url, stream=True, timeout=600)
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(1024*1024):
                f.write(chunk)
        with zipfile.ZipFile(zip_path) as z:
            tif_files = [f for f in z.namelist() if f.endswith('.tif')]

            if not tif_files:
                print('  > No tif in ZIP.')
                return

            # Extract all tifs
            for tif in tif_files:
                z.extract(tif, tmp_dir)

            # Order according to requested bands if possible
            ordered_tifs = []
            for b in bands:
                match = next((t for t in tif_files if b in os.path.basename(t)), None)
                if match:
                    ordered_tifs.append(match)

            if len(ordered_tifs) != len(bands):
                ordered_tifs = tif_files  # fallback

            first = os.path.join(tmp_dir, ordered_tifs[0])
            with rasterio.open(first) as src0:
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
        print(f'  > Export error {out_path}: {e}')
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

# Resample util from GLDAS / ERA5

def resample_to_match(src_path, ref_path):
    try:
        with rasterio.open(ref_path) as ref:
            ref_meta = ref.meta.copy()
        with rasterio.open(src_path) as src:
            if src.width == ref_meta['width'] and src.height == ref_meta['height'] and src.transform == ref_meta['transform']:
                return
            ref_meta.update({'count': src.count, 'dtype': src.dtypes[0], 'nodata': src.nodata})
            temp = src_path + '.tmp.tif'
            with rasterio.open(temp, 'w', **ref_meta) as dst:
                for i in range(1, src.count+1):
                    rasterio.warp.reproject(
                        source=rasterio.band(src,i),
                        destination=rasterio.band(dst,i),
                        src_crs=src.crs,
                        src_transform=src.transform,
                        dst_crs=ref_meta['crs'],
                        dst_transform=ref_meta['transform'],
                        resampling=rasterio.warp.Resampling.bilinear)
            shutil.move(temp, src_path)
    except Exception as e:
        print(f'  > Resample fail {os.path.basename(src_path)}: {e}')

# ================================
# Core
# ================================

def download_fldas(lst_folder, out_folder):
    os.makedirs(out_folder, exist_ok=True)
    sample = next(Path(lst_folder).glob('*.tif'), None)
    if not sample:
        print(f'No sample tif in {lst_folder}.')
        return
    region = get_roi_coords_from_tif(str(sample))
    geom = ee.Geometry.Polygon(region, proj=TARGET_CRS, evenOdd=False)
    dates = get_dates(lst_folder)
    if not dates:
        print('No dates found to process.')
        return
    print(f'FLDAS: {len(dates)} dates to process.')
    for d in dates:
        date_str = d.strftime('%Y-%m-%d')
        out_path = os.path.join(out_folder, f'FLDAS_{date_str}.tif')
        if SKIP_EXISTING and os.path.exists(out_path) and verify(out_path):
            resample_to_match(out_path, str(sample))
            print(f'Skip verified {os.path.basename(out_path)}')
            continue
        try:
            img = ee.ImageCollection(FLDAS_COLLECTION) \
                    .filterDate(date_str, (d + timedelta(days=30)).strftime('%Y-%m-%d')) \
                    .filterBounds(geom) \
                    .first()
            if img is None:
                print(f'  > No FLDAS image {date_str}')
                continue
            export_image(img, FLDAS_BANDS, region, out_path)
            if os.path.exists(out_path):
                resample_to_match(out_path, str(sample))
            time.sleep(0.5)
        except ee.EEException as eee:
            print(f'EE error {date_str}: {eee}')
        except Exception as ex:
            print(f'Error {date_str}: {ex}')

if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Download FLDAS soil temperature bands.')
    p.add_argument('--lst_folder', required=True)
    p.add_argument('--out_folder', required=True)
    a = p.parse_args()
    download_fldas(a.lst_folder, a.out_folder) 