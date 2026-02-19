import ee
import os
import json
import time
import glob
import hashlib
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- 1. AUTHENTICATION ---
# We use the key you stored in GitHub Secrets
key = os.environ.get('EE_KEY')
ee_project = os.environ.get('EE_PROJECT')
print('Local run: set $env:EE_PROJECT="snowcast-1" then python main.py')
if not ee_project:
    raise ValueError('EE_PROJECT is required. PowerShell: $env:EE_PROJECT="snowcast-1"; python main.py')
if key:
    key_json = json.loads(key)
    client_email = key_json.get('client_email')
    print(f'EE_KEY client_email: {client_email}')
    creds = ee.ServiceAccountCredentials(client_email, key_data=key)
    ee.Initialize(creds, project=ee_project)
else:
    ee.Initialize(project=ee_project)  # Local fallback

# --- 2. SETUP ---
ASSET = 'projects/gcp-public-data-weathernext/assets/weathernext_2_0_0'
# Use ERA5 hourly (includes pressure-level fields); monthly climatology is built by month filtering + mean.
ERA5_ASSET = 'ECMWF/ERA5/HOURLY'
ERA5_GEOPOTENTIAL_BAND = 'geopotential'
WN2_Z500_BAND = '500_geopotential'
OUTPUT = 'public'  # The folder that becomes the website
os.makedirs(OUTPUT, exist_ok=True)
DEBUG_BANDS = os.environ.get('DEBUG_BANDS') == '1'
TARGET_CRS = 'EPSG:4326'
TARGET_SCALE = 100000
CLIMO_ANCHOR_YEARS = [1991, 2020]
CLIMO_ANCHOR_DAY = 15


def ts():
    return time.strftime('%Y-%m-%d %H:%M:%S')


# Define Region: Northern Hemisphere for Z500 anomaly (avoid exact pole/antimeridian edges)
NH_W = ee.Geometry.Rectangle([-180.0, 20.0, 0.0, 89.5], geodesic=False)
NH_E = ee.Geometry.Rectangle([0.0, 20.0, 180.0, 89.5], geodesic=False)
NH_REGION = NH_W.union(NH_E, maxError=1)

# Forecast Steps to generate (Hours out)
HOURS = [0, 6, 12, 18, 24, 30, 36, 42, 48, 60, 72]


def cleanup_old_products():
    stale_patterns = ['vort_*.jpg', 'radar_*.jpg', 'snow_*.jpg', 'z500_*.jpg', 'z500a_*.jpg']
    removed = 0
    for pattern in stale_patterns:
        for path in glob.glob(os.path.join(OUTPUT, pattern)):
            os.remove(path)
            removed += 1
    print(f'[{ts()}] Removed {removed} stale product image(s).')


def filter_forecast_hour(ic, h):
    return ic.filter(
        ee.Filter.Or(
            ee.Filter.eq('forecast_hour', h),
            ee.Filter.eq('forecast_hour', str(h)),
        )
    )


def clip_to_nh(image):
    return ee.ImageCollection([image.clip(NH_W), image.clip(NH_E)]).mosaic()


def download_thumb(ee_image, out_path, vis_params):
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    url = ee_image.getThumbURL(vis_params)

    try:
        with requests.get(url, stream=True, timeout=300) as response:
            if response.status_code != 200:
                body = response.text[:500]
                print(f'[{ts()}] Thumbnail download failed: status={response.status_code}, body={body}')
                raise requests.HTTPError(
                    f'Thumbnail download failed with status {response.status_code}: {body}',
                    response=response
                )

            with open(out_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
    except Exception as e:
        print(f'[{ts()}] Thumbnail download error for {out_path}: {e}')
        raise


def get_latest_start_time_recent(ic, days=7):
    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%S')
    window_end = (now_utc + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    recent = ic.filterDate(window_start, window_end)
    print(f'[{ts()}] Using WN2 recency window: {window_start}Z to {window_end}Z')

    print(f'[{ts()}] Fetching start_time...')
    t0 = time.time()
    latest_start_time = recent.aggregate_max('start_time').getInfo()
    print(f'[{ts()}] Fetched start_time in {time.time() - t0:.2f}s: {latest_start_time}')

    if latest_start_time is not None:
        latest_run_collection = recent.filter(ee.Filter.eq('start_time', latest_start_time))
        return latest_start_time, ee.Date(latest_start_time), recent, latest_run_collection

    print(f'[{ts()}] start_time unavailable in recent subset, falling back to system:time_start.')
    t0 = time.time()
    latest_system_time = recent.aggregate_max('system:time_start').getInfo()
    print(f'[{ts()}] Fetched fallback system:time_start in {time.time() - t0:.2f}s: {latest_system_time}')

    if latest_system_time is None:
        raise ValueError('No WN2 images found in the last 7 days.')

    latest_start_date = ee.Date(latest_system_time)
    latest_start_time = latest_start_date.format("YYYY-MM-dd'T'HH:mm:ss'Z'").getInfo()
    latest_run_collection = recent.filter(ee.Filter.eq('system:time_start', latest_system_time))
    return latest_start_time, latest_start_date, recent, latest_run_collection


# Get the latest run from a recent time window (avoid expensive full-collection latest query)
collection = ee.ImageCollection(ASSET)
latest_start_time, latest_start_date, recent_collection, latest_start_collection = get_latest_start_time_recent(collection, days=7)

if latest_start_collection.size().getInfo() == 0:
    raise ValueError('Latest WN2 run filter returned no images in the last 7 days.')

hour0_candidates = filter_forecast_hour(latest_start_collection, 0)
hour0_image = ee.Image(ee.Algorithms.If(hour0_candidates.size().gt(0), hour0_candidates.first(), latest_start_collection.first()))

print(f'[{ts()}] Fetching system:index...')
t0 = time.time()
run_date = hour0_image.get('system:index').getInfo()
if run_date is None:
    run_date = latest_start_time
print(f'[{ts()}] Fetched system:index in {time.time() - t0:.2f}s: {run_date}')

print(f'Processing Run: {run_date}')
print(f'Latest start_time: {latest_start_time}')

if DEBUG_BANDS:
    print(f'[{ts()}] Fetching bandNames...')
    t0 = time.time()
    band_names = ee.List(hour0_image.bandNames()).getInfo()
    print(f'[{ts()}] Fetched bandNames in {time.time() - t0:.2f}s ({len(band_names)} bands)')
    print(f'Available bands: {band_names}')
    if WN2_Z500_BAND not in band_names:
        raise ValueError(f'Required WN2 band missing: {WN2_Z500_BAND}')
else:
    has_z500_band = hour0_image.bandNames().contains(WN2_Z500_BAND).getInfo()
    if not has_z500_band:
        raise ValueError(f'Required WN2 band missing: {WN2_Z500_BAND}')
    print(f'[{ts()}] DEBUG_BANDS not set; skipping full bandNames print.')

print(f'[{ts()}] Checking ERA5 climatology source...')
t0 = time.time()
era5_sample = ee.ImageCollection(ERA5_ASSET).first()
if era5_sample is None:
    raise ValueError(f'ERA5 source is empty: {ERA5_ASSET}')

if DEBUG_BANDS:
    era5_band_names = ee.List(era5_sample.bandNames()).getInfo()
    print(f'[{ts()}] ERA5 sample bands ({len(era5_band_names)}): {era5_band_names}')
    if ERA5_GEOPOTENTIAL_BAND not in era5_band_names:
        raise ValueError(f'Required ERA5 band missing: {ERA5_GEOPOTENTIAL_BAND}')
else:
    has_era5_geopotential = era5_sample.bandNames().contains(ERA5_GEOPOTENTIAL_BAND).getInfo()
    if not has_era5_geopotential:
        raise ValueError(f'Required ERA5 band missing: {ERA5_GEOPOTENTIAL_BAND}')
    print(f'[{ts()}] DEBUG_BANDS not set; skipping full ERA5 bandNames print.')
print(f'[{ts()}] ERA5 climatology source ready in {time.time() - t0:.2f}s.')


def get_hour_image(h):
    hour_filtered = filter_forecast_hour(latest_start_collection, h)
    return ee.Image(ee.Algorithms.If(hour_filtered.size().gt(0), hour_filtered.first(), hour0_image))


# --- 3. METEOROLOGY LOGIC ---
climatology_cache = {}


def get_climatology_height_dam(valid_time, match_hour=False):
    # v1: monthly climatology. Function signature leaves room for hour matching later.
    month = int(ee.Number(valid_time.get('month')).getInfo())
    cache_key = f'month_{month}_hourmatch_{int(match_hour)}'

    if cache_key not in climatology_cache:
        print(f'[{ts()}] Building ERA5 monthly climatology proxy for month={month}...')
        t0 = time.time()
        clim_hour = 0
        if match_hour:
            # Placeholder for future hour-of-day matching logic.
            clim_hour = int(ee.Number(valid_time.get('hour')).getInfo())

        anchor_images = []
        for year in CLIMO_ANCHOR_YEARS:
            anchor_date = ee.Date.fromYMD(year, month, CLIMO_ANCHOR_DAY)
            anchor = (
                ee.ImageCollection(ERA5_ASSET)
                .filterDate(anchor_date, anchor_date.advance(1, 'day'))
                .filter(ee.Filter.calendarRange(clim_hour, clim_hour, 'hour'))
                .select(ERA5_GEOPOTENTIAL_BAND)
                .first()
            )
            anchor_images.append(anchor)

        climatology_cache[cache_key] = (
            clip_to_nh(
                ee.ImageCollection.fromImages(anchor_images)
                .mean()
                .divide(9.80665)
                .divide(10)
            )
            .resample('bilinear')
        )
        print(
            f'[{ts()}] Prepared ERA5 climatology proxy for month={month} '
            f'using anchor years {CLIMO_ANCHOR_YEARS} in {time.time() - t0:.2f}s (deferred compute).'
        )

    return climatology_cache[cache_key]


def generate_z500_anomaly(img, h):
    # Forecast 500 hPa geopotential -> height (dam)
    forecast_height_dam = (
        clip_to_nh(
            img.select(WN2_Z500_BAND)
            .divide(9.80665)
            .divide(10)
            .reduceResolution(reducer=ee.Reducer.mean(), bestEffort=True, maxPixels=1024)
        )
        .resample('bilinear')
    )

    # valid_time = start_time + forecast_hour
    valid_time = latest_start_date.advance(h, 'hour')
    climatology_height_dam = get_climatology_height_dam(valid_time, match_hour=False)
    z500_anomaly = forecast_height_dam.subtract(climatology_height_dam)

    vis = {
        'min': 0,
        'max': 600,
        'palette': ['#08306b', '#2171b5', '#6baed6', '#f7f7f7', '#fcae91', '#fb6a4a', '#cb181d']
    }
    out_file = f'{OUTPUT}/z500a_{h:03d}.jpg'
    print(f'[{ts()}] Hour {h}: exporting {out_file}...')
    t0 = time.time()
    thumb_image = z500_anomaly.visualize(**vis).reproject(
        crs='EPSG:4326',
        crsTransform=[0.25, 0.0, -180.0, 0.0, -0.25, 90.0],
    )
    thumb_params = {
        'region': [-180.0, 20.0, 180.0, 89.5],
        'dimensions': 1600,
        'format': 'jpg',
    }
    try:
        download_thumb(thumb_image, out_file, thumb_params)
    except requests.HTTPError as e:
        response = getattr(e, 'response', None)
        status = response.status_code if response is not None else None
        body = response.text if response is not None else str(e)
        if status == 400 and 'User memory limit exceeded' in body:
            print(f'[{ts()}] Hour {h}: retrying with coarser thumbnail params due to memory limit...')
            retry_params = dict(thumb_params)
            retry_params['dimensions'] = 1000
            try:
                download_thumb(thumb_image, out_file, retry_params)
            except requests.HTTPError as retry_error:
                retry_response = getattr(retry_error, 'response', None)
                retry_body = retry_response.text[:500] if retry_response is not None else str(retry_error)
                raise RuntimeError(
                    f'Hour {h} thumbnail retry failed (status={getattr(retry_response, "status_code", "n/a")}): {retry_body}'
                ) from retry_error
        else:
            raise
    print(f'[{ts()}] Hour {h}: z500 anomaly done ({time.time() - t0:.2f}s)')


def _file_md5(p: Path) -> str:
    h = hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sanity_check_jpgs(out_dir: str, pattern: str = "z500a_*.jpg") -> None:
    from PIL import Image
    files = sorted(Path(out_dir).glob(pattern))
    if not files:
        raise RuntimeError(f"No files found matching {pattern} in {out_dir}")

    hashes = []
    sizes = []
    for p in files:
        with Image.open(p) as im:
            sizes.append((p.name, im.size))
        hashes.append((p.name, _file_md5(p)))

    tiny = [(n, s) for n, s in sizes if s[0] < 300 or s[1] < 150]
    if tiny:
        raise RuntimeError(f"Sanity check failed: tiny images detected: {tiny[:5]}")

    unique_hashes = {h for _, h in hashes}
    if len(unique_hashes) == 1:
        raise RuntimeError("Sanity check failed: all images are identical (same MD5).")

    print(f"Sanity OK: {len(files)} images, {len(unique_hashes)} unique hashes.")


# --- 4. EXECUTION ---
cleanup_old_products()
failures = []
for h in HOURS:
    print(f'Generating Hour {h}...')
    try:
        img = get_hour_image(h)
        generate_z500_anomaly(img, h)
    except Exception as e:
        print(f'[{ts()}] Hour {h}: FAILED - {e}')
        failures.append((h, str(e)))

if failures:
    print(f'[{ts()}] Completed with {len(failures)} failed hour(s).')
    for h, msg in failures:
        print(f'[{ts()}] Failure summary - hour {h}: {msg}')

sanity_check_jpgs("public")


# --- 5. BUILD INTERFACE ---
html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>WN2 Z500 Anomaly Viewer</title>
    <style>
        body {{ background:#222; color:#eee; font-family:sans-serif; text-align:center; }}
        img {{ max-width:100%; height:auto; border:1px solid #555; }}
        button {{ padding:10px 20px; font-size:16px; cursor:pointer; }}
    </style>
</head>
<body>
    <h2>WeatherNext 2 Z500 Height Anomaly (NH): {run_date}</h2>

    <div>
        <button onclick="change(-1)">Prev</button>
        <span id="label" style="display:inline-block; width:80px; font-weight:bold;">Hour 00</span>
        <button onclick="change(1)">Next</button>
    </div>

    <br>
    <img id="map" src="z500a_000.jpg">

    <script>
        let hours = {HOURS};
        let idx = 0;

        function update() {{
            let h = hours[idx].toString().padStart(3, '0');
            document.getElementById('map').src = 'z500a_' + h + '.jpg';
            document.getElementById('label').innerText = 'Hour ' + h;
        }}

        function change(dir) {{
            idx = (idx + dir + hours.length) % hours.length;
            update();
        }}
    </script>
</body>
</html>
"""

with open(f'{OUTPUT}/index.html', 'w') as f:
    f.write(html)
