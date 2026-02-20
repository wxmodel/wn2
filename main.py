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
WN2_Z500_BAND = '500_geopotential'
WN2_500_U_BAND = '500_u_component_of_wind'
WN2_500_V_BAND = '500_v_component_of_wind'
WN2_MSLP_BAND = 'mean_sea_level_pressure'
WN2_PRECIP_6H_BAND = 'total_precipitation_6hr'
WN2_T2M_BAND = '2m_temperature'
WN2_T850_BAND = '850_temperature'
WN2_T700_BAND = '700_temperature'
OUTPUT = 'public'  # The folder that becomes the website
os.makedirs(OUTPUT, exist_ok=True)
DEBUG_BANDS = os.environ.get('DEBUG_BANDS') == '1'
TARGET_CRS = 'EPSG:4326'
ANOMALY_DIMS = '1200x880'
CONUS_DIMS = '1400x1000'

# Regions
NH_W = ee.Geometry.Rectangle([-180.0, 20.0, 0.0, 89.5], geodesic=False)
NH_E = ee.Geometry.Rectangle([0.0, 20.0, 180.0, 89.5], geodesic=False)
NH_REGION = NH_W.union(NH_E, maxError=1)
NA_REGION = ee.Geometry.Rectangle([-170.0, 10.0, -45.0, 80.0], geodesic=False)
CONUS_REGION = ee.Geometry.Rectangle([-127.0, 22.0, -65.0, 50.0], geodesic=False)
NH_THUMB_REGION = [-180.0, 8.0, 20.0, 88.0]
NA_THUMB_REGION = [-170.0, 8.0, -40.0, 82.0]
CONUS_THUMB_REGION = [-127.0, 22.0, -65.0, 50.0]

# Boundaries overlays
COUNTRIES = ee.FeatureCollection('USDOS/LSIB_SIMPLE/2017')
US_STATES = ee.FeatureCollection('TIGER/2018/States')


def ts():
    return time.strftime('%Y-%m-%d %H:%M:%S')

# Forecast Steps to generate (Hours out)
HOURS = [0, 6, 12, 18, 24, 30, 36, 42, 48, 60, 72]
hours_csv = os.environ.get('HOURS_CSV')
if hours_csv:
    HOURS = [int(x.strip()) for x in hours_csv.split(',') if x.strip()]
    print(f'[{ts()}] HOURS override from HOURS_CSV: {HOURS}')


def cleanup_old_products():
    stale_patterns = [
        'z500a_*.jpg',
        'nh_z500a_*.jpg',
        'na_z500a_*.jpg',
        'conus_mslp_ptype_*.jpg',
        'conus_vort500_*.jpg',
    ]
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


def parse_run_init_utc(ts_utc):
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_utc, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


RUN_INIT_UTC = parse_run_init_utc(latest_start_time)


def format_map_times(hour):
    valid_utc = RUN_INIT_UTC + timedelta(hours=hour)
    init_text = RUN_INIT_UTC.strftime('%H00 UTC %a %d %b %Y')
    valid_text = valid_utc.strftime('%H00 UTC %a %d %b %Y')
    return init_text, valid_text

if DEBUG_BANDS:
    print(f'[{ts()}] Fetching bandNames...')
    t0 = time.time()
    band_names = ee.List(hour0_image.bandNames()).getInfo()
    print(f'[{ts()}] Fetched bandNames in {time.time() - t0:.2f}s ({len(band_names)} bands)')
    print(f'Available bands: {band_names}')
else:
    band_names = ee.List(hour0_image.bandNames()).getInfo()
    print(f'[{ts()}] DEBUG_BANDS not set; skipping full bandNames print.')

required_bands = [
    WN2_Z500_BAND,
    WN2_500_U_BAND,
    WN2_500_V_BAND,
    WN2_MSLP_BAND,
    WN2_PRECIP_6H_BAND,
    WN2_T2M_BAND,
    WN2_T850_BAND,
    WN2_T700_BAND,
]
missing_bands = [b for b in required_bands if b not in band_names]
if missing_bands:
    raise ValueError(f'Required WN2 bands missing: {missing_bands}')


def get_hour_image(h):
    hour_filtered = filter_forecast_hour(latest_start_collection, h)
    return ee.Image(ee.Algorithms.If(hour_filtered.size().gt(0), hour_filtered.first(), hour0_image))


# --- 3. METEOROLOGY LOGIC ---
def contour_overlay(field, interval, color, opacity=0.82):
    # Draw smooth-ish contour lines by finding quantization edges.
    smoothed = field.resample('bilinear').focalMean(1, 'circle', 'pixels')
    quantized = smoothed.divide(interval).round()
    edges = quantized.focalMax(1).neq(quantized.focalMin(1)).focalMax(1).selfMask()
    return edges.visualize(palette=[color], opacity=opacity)


def border_overlay(include_states=False):
    country_lines = ee.Image().byte().paint(COUNTRIES, 1, 1).selfMask().visualize(palette=['#333333'])
    if include_states:
        state_lines = ee.Image().byte().paint(US_STATES, 1, 1).selfMask().visualize(palette=['#6b4a2c'])
        return ee.ImageCollection([country_lines, state_lines]).mosaic()
    return country_lines


def basemap_overlay(region_geom, land_color='#ececec', ocean_color='#cfe0ea'):
    ocean = ee.Image.constant(1).clip(region_geom).visualize(palette=[ocean_color], opacity=1.0)
    land_mask = ee.Image().byte().paint(COUNTRIES, 1, 1).clip(region_geom).selfMask()
    land = land_mask.visualize(palette=[land_color], opacity=1.0)
    return ee.ImageCollection([ocean, land]).mosaic()


def pseudo_z500_anomaly_m(height_dam, radius_px=24):
    # Remove the broad background field so synoptic anomalies stand out.
    broad = height_dam.resample('bilinear').focalMean(radius=radius_px, kernelType='circle', units='pixels')
    return height_dam.subtract(broad).multiply(10)


def shrink_dimensions(dimensions):
    if isinstance(dimensions, int):
        return max(500, int(dimensions * 0.7))
    if isinstance(dimensions, str) and 'x' in dimensions:
        w_str, h_str = dimensions.lower().split('x', 1)
        try:
            w = int(w_str)
            h = int(h_str)
            return f'{max(500, int(w * 0.7))}x{max(400, int(h * 0.7))}'
        except ValueError:
            return dimensions
    return dimensions


def load_font(size, bold=False):
    from PIL import ImageFont

    candidates = []
    if bold:
        candidates.extend([
            'DejaVuSans-Bold.ttf',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
            'arialbd.ttf',
        ])
    else:
        candidates.extend([
            'DejaVuSans.ttf',
            '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
            'arial.ttf',
        ])
    for font_name in candidates:
        try:
            return ImageFont.truetype(font_name, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def annotate_map_file(out_file, product_key, hour):
    from PIL import Image, ImageDraw

    product_titles = {
        'nh_z500a': 'WN2 0.25° | 500-hPa Geopotential Height (dam) & Anomaly (m) | Northern Hemisphere',
        'na_z500a': 'WN2 0.25° | 500-hPa Geopotential Height (dam) & Anomaly (m) | North America',
        'conus_mslp_ptype': 'WN2 0.25° | MSLP (hPa) + 500-hPa Height (dam) + Precip Type | CONUS',
        'conus_vort500': 'WN2 0.25° | 500-hPa Relative Vorticity + 500-hPa Height (dam) | CONUS',
    }
    title = product_titles.get(product_key, product_key)
    init_text, valid_text = format_map_times(hour)
    subtitle = f'Init: {init_text} | Hour: [{hour:03d}] | Valid: {valid_text}'

    with Image.open(out_file) as src:
        img = src.convert('RGB')
        header_h = 78
        footer_h = 20
        canvas = Image.new('RGB', (img.width, img.height + header_h + footer_h), color=(236, 236, 236))
        canvas.paste(img, (0, header_h))
        draw = ImageDraw.Draw(canvas)
        title_font = load_font(30 if img.width >= 1300 else 26, bold=True)
        subtitle_font = load_font(22 if img.width >= 1300 else 19, bold=False)
        footer_font = load_font(15 if img.width >= 1300 else 13, bold=False)

        draw.text((12, 10), title, fill=(20, 20, 20), font=title_font)
        draw.text((12, 44), subtitle, fill=(25, 25, 25), font=subtitle_font)
        footer_text = f'Run: {run_date} | Source: WeatherNext2 (Earth Engine)'
        draw.text((12, img.height + header_h + 2), footer_text, fill=(35, 35, 35), font=footer_font)
        draw.rectangle((0, header_h, img.width - 1, header_h + img.height - 1), outline=(32, 32, 32), width=2)
        canvas.save(out_file, format='JPEG', quality=95)


def export_composite(composite, out_file, region, dimensions=1600, scale=None):
    print(f'[{ts()}] Exporting {out_file}...')
    t0 = time.time()
    params = {
        'region': region,
        'format': 'jpg',
    }
    if scale is not None:
        params['crs'] = TARGET_CRS
        params['scale'] = scale
    else:
        params['dimensions'] = dimensions
    try:
        download_thumb(composite, out_file, params)
    except requests.HTTPError as e:
        response = getattr(e, 'response', None)
        status = response.status_code if response is not None else None
        body = response.text if response is not None else str(e)
        if status == 400 and 'User memory limit exceeded' in body:
            print(f'[{ts()}] Retrying {out_file} with smaller dimensions due to memory limit...')
            retry_params = dict(params)
            if scale is not None:
                retry_params['scale'] = int(scale * 1.4)
            else:
                retry_params['dimensions'] = shrink_dimensions(dimensions)
            download_thumb(composite, out_file, retry_params)
        else:
            raise
    print(f'[{ts()}] Export complete for {out_file} ({time.time() - t0:.2f}s)')


def generate_z500_anomaly_map(img, h, region, prefix):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    anomaly_radius = 30 if prefix == 'nh_z500a' else 20
    anomaly_min = -100 if prefix == 'nh_z500a' else -100
    anomaly_max = 100 if prefix == 'nh_z500a' else 100
    forecast_height_dam = (
        img.select(WN2_Z500_BAND)
        .divide(9.80665)
        .divide(10)
        .clip(region_geom)
    )
    anomaly_m = pseudo_z500_anomaly_m(forecast_height_dam, radius_px=anomaly_radius)

    anomaly_layer = anomaly_m.visualize(
        min=anomaly_min,
        max=anomaly_max,
        palette=[
            '#6a00a8', '#9c4dcc', '#5e60ce', '#2f80ed', '#7dcfff',
            '#f7f7f7',
            '#ffe08a', '#ffad5a', '#ff6b3a', '#d7301f', '#7f0000'
        ],
    )
    z500_contours = contour_overlay(forecast_height_dam, interval=6, color='#1f1f1f', opacity=0.84)
    overlays = [
        basemap_overlay(region_geom, land_color='#d8d8d8', ocean_color='#d9e5ee'),
        anomaly_layer,
        z500_contours,
        border_overlay(include_states=False),
    ]
    composite = ee.ImageCollection(overlays).mosaic()

    out_file = f'{OUTPUT}/{prefix}_{h:03d}.jpg'
    map_dims = ANOMALY_DIMS
    export_composite(
        composite,
        out_file,
        region,
        dimensions=map_dims,
    )
    annotate_map_file(out_file, prefix, h)


def generate_mslp_ptype_map(img, h):
    region_geom = ee.Geometry.Rectangle(CONUS_THUMB_REGION, geodesic=False)
    precip_rate = img.select(WN2_PRECIP_6H_BAND).multiply(1000).divide(6).focalMean(2, 'circle', 'pixels')  # mm/hr
    precip_mask = precip_rate.gt(0.35)

    t2c = img.select(WN2_T2M_BAND).subtract(273.15)
    t850c = img.select(WN2_T850_BAND).subtract(273.15)
    t700c = img.select(WN2_T700_BAND).subtract(273.15)

    snow = precip_mask.And(t2c.lte(1)).And(t850c.lte(-1)).And(t700c.lte(-2))
    freezing_rain = precip_mask.And(t2c.lte(0)).And(t850c.gt(1)).And(t700c.gt(-2))
    sleet = precip_mask.And(t2c.lte(0)).And(t850c.gt(0)).And(t700c.lte(-2)).And(freezing_rain.Not())
    rain = precip_mask.And(snow.Not()).And(freezing_rain.Not()).And(sleet.Not())

    rain_layer = precip_rate.updateMask(rain).visualize(
        min=0.1, max=25,
        palette=['#9be564', '#4caf50', '#2e7d32', '#ffe600', '#ff9800', '#e53935', '#b71c1c'],
    )
    snow_layer = precip_rate.updateMask(snow).visualize(
        min=0.1, max=25,
        palette=['#c7ebff', '#7fd4ff', '#4aa3df', '#1f78b4', '#0d47a1', '#4a148c'],
    )
    frz_layer = precip_rate.updateMask(freezing_rain).visualize(
        min=0.1, max=25,
        palette=['#ffc4da', '#f06292', '#d81b60', '#ad1457', '#880e4f'],
    )
    sleet_layer = precip_rate.updateMask(sleet).visualize(
        min=0.1, max=25,
        palette=['#e1bee7', '#ce93d8', '#ab47bc', '#8e24aa', '#6a1b9a'],
    )

    mslp_hpa = img.select(WN2_MSLP_BAND).divide(100).clip(region_geom)
    mslp_contours = contour_overlay(
        mslp_hpa,
        interval=3,
        color='#2a2a2a',
        opacity=0.9,
    )
    z500_height_dam = img.select(WN2_Z500_BAND).divide(9.80665).divide(10).clip(region_geom)
    z500_contours = contour_overlay(
        z500_height_dam,
        interval=6,
        color='#444444',
        opacity=0.82,
    )

    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color='#eeeeee', ocean_color='#c7dbe6'),
        rain_layer,
        snow_layer,
        frz_layer,
        sleet_layer,
        mslp_contours,
        z500_contours,
        border_overlay(include_states=True),
    ]).mosaic()

    out_file = f'{OUTPUT}/conus_mslp_ptype_{h:03d}.jpg'
    export_composite(composite, out_file, CONUS_THUMB_REGION, dimensions=CONUS_DIMS)
    annotate_map_file(out_file, 'conus_mslp_ptype', h)


def generate_vort500_map(img, h):
    region_geom = ee.Geometry.Rectangle(CONUS_THUMB_REGION, geodesic=False)
    u = img.select(WN2_500_U_BAND).resample('bilinear').focalMean(2, 'circle', 'pixels')
    v = img.select(WN2_500_V_BAND).resample('bilinear').focalMean(2, 'circle', 'pixels')
    du_dy_deg = u.gradient().select('y')
    dv_dx_deg = v.gradient().select('x')

    lat = ee.Image.pixelLonLat().select('latitude').multiply(3.141592653589793 / 180.0)
    # gradient() is per-pixel; convert using approximate 0.25-degree grid spacing.
    meters_per_px_lat = ee.Image.constant(27830.0)
    meters_per_px_lon = lat.cos().multiply(27830.0).max(5000.0)
    du_dy = du_dy_deg.divide(meters_per_px_lat)
    dv_dx = dv_dx_deg.divide(meters_per_px_lon)
    vort_1e5 = dv_dx.subtract(du_dy).multiply(1e5)
    vort_display = vort_1e5.multiply(50000.0).focalMean(2, 'circle', 'pixels')

    vort_layer = vort_display.updateMask(vort_display.gt(6)).visualize(
        min=6, max=50,
        palette=['#f5ee00', '#f4c236', '#ee8c4a', '#d35a75', '#a03ca0', '#5f209f'],
    )
    z500_height_dam = img.select(WN2_Z500_BAND).divide(9.80665).divide(10).clip(region_geom)
    z500_contours = contour_overlay(
        z500_height_dam,
        interval=6,
        color='#2d2d2d',
        opacity=0.88,
    )
    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color='#eeeeee', ocean_color='#c7dbe6'),
        vort_layer,
        z500_contours,
        border_overlay(include_states=True),
    ]).mosaic()

    out_file = f'{OUTPUT}/conus_vort500_{h:03d}.jpg'
    export_composite(composite, out_file, CONUS_THUMB_REGION, dimensions=CONUS_DIMS)
    annotate_map_file(out_file, 'conus_vort500', h)


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
    if len(files) > 1 and len(unique_hashes) == 1:
        raise RuntimeError("Sanity check failed: all images are identical (same MD5).")

    print(f"Sanity OK: {len(files)} images, {len(unique_hashes)} unique hashes.")


# --- 4. EXECUTION ---
cleanup_old_products()
failures = []
successful_exports = 0
product_patterns = [
    'nh_z500a_*.jpg',
    'na_z500a_*.jpg',
    'conus_mslp_ptype_*.jpg',
    'conus_vort500_*.jpg',
]

for h in HOURS:
    print(f'Generating Hour {h}...')
    img = get_hour_image(h)
    tasks = [
        ('nh_z500a', lambda: generate_z500_anomaly_map(img, h, NH_THUMB_REGION, 'nh_z500a')),
        ('na_z500a', lambda: generate_z500_anomaly_map(img, h, NA_THUMB_REGION, 'na_z500a')),
        ('conus_mslp_ptype', lambda: generate_mslp_ptype_map(img, h)),
        ('conus_vort500', lambda: generate_vort500_map(img, h)),
    ]
    for name, fn in tasks:
        try:
            fn()
            successful_exports += 1
        except Exception as e:
            err_msg = str(e)
            print(f'[{ts()}] Hour {h} product {name}: FAILED - {err_msg}')
            failures.append((f'{h}:{name}', err_msg))
            if 'earthengine.thumbnails.create' in err_msg:
                raise RuntimeError(
                    "Earth Engine permission denied: earthengine.thumbnails.create. "
                    "Grant the service account Earth Engine User (or Admin) on EE_PROJECT and ensure Earth Engine API is enabled."
                ) from e

if failures:
    print(f'[{ts()}] Completed with {len(failures)} failed hour(s).')
    for h, msg in failures:
        print(f'[{ts()}] Failure summary - hour {h}: {msg}')

if successful_exports > 0:
    for pattern in product_patterns:
        sanity_check_jpgs("public", pattern=pattern)
else:
    print(f'[{ts()}] Skipping sanity check: no product images were created.')


# --- 5. BUILD INTERFACE ---
html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>WN2 Multi-Product Viewer</title>
    <style>
        body {{ background:#1f1f1f; color:#efefef; font-family:system-ui, sans-serif; text-align:center; margin:0; }}
        .wrap {{ max-width:1200px; margin:0 auto; padding:18px; }}
        img {{ max-width:100%; height:auto; border:1px solid #4f4f4f; background:#111; }}
        button {{ padding:10px 20px; font-size:16px; cursor:pointer; }}
        select {{ padding:8px 10px; font-size:15px; }}
        .controls {{ display:flex; gap:12px; justify-content:center; align-items:center; flex-wrap:wrap; margin-bottom:14px; }}
    </style>
</head>
<body>
    <div class="wrap">
    <h2>WeatherNext 2 Map Viewer: {run_date}</h2>

    <div class="controls">
        <label for="product">Map:</label>
        <select id="product" onchange="update()">
            <option value="nh_z500a">NH 500mb Height Anomaly</option>
            <option value="na_z500a">North America 500mb Height Anomaly</option>
            <option value="conus_mslp_ptype">CONUS MSLP + P-Type</option>
            <option value="conus_vort500">CONUS 500mb Vorticity</option>
        </select>
        <button onclick="change(-1)">Prev</button>
        <span id="label" style="display:inline-block; width:120px; font-weight:bold;">Hour 000</span>
        <button onclick="change(1)">Next</button>
    </div>

    <img id="map" src="nh_z500a_000.jpg">

    <script>
        let hours = {HOURS};
        let idx = 0;

        function update() {{
            let h = hours[idx].toString().padStart(3, '0');
            let product = document.getElementById('product').value;
            document.getElementById('map').src = product + '_' + h + '.jpg';
            document.getElementById('label').innerText = 'Hour ' + h;
        }}

        function change(dir) {{
            idx = (idx + dir + hours.length) % hours.length;
            update();
        }}
    </script>
    </div>
</body>
</html>
"""

with open(f'{OUTPUT}/index.html', 'w') as f:
    f.write(html)
