import ee
import os
import json
import time
import glob
import hashlib
import math
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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
NH_SOURCE_REGION = [-179.5, 20.0, 179.5, 89.0]
NH_W_BOUNDS = [-180.0, 20.0, 0.0, 89.0]
NH_E_BOUNDS = [0.0, 20.0, 180.0, 89.0]
try:
    NH_LON0 = float(os.environ.get('NH_LON0', '80.0'))
except ValueError:
    NH_LON0 = 80.0

# Regions
NH_W = ee.Geometry.Rectangle([-180.0, 20.0, 0.0, 89.5], geodesic=False)
NH_E = ee.Geometry.Rectangle([0.0, 20.0, 180.0, 89.5], geodesic=False)
NH_REGION = NH_W.union(NH_E, maxError=1)
NA_REGION = ee.Geometry.Rectangle([-170.0, 10.0, -45.0, 80.0], geodesic=False)
CONUS_REGION = ee.Geometry.Rectangle([-127.0, 22.0, -65.0, 50.0], geodesic=False)
WORLD_REGION = ee.Geometry.Rectangle([-180.0, -89.9, 180.0, 89.9], geodesic=False)
NH_THUMB_REGION = [-180.0, 8.0, 20.0, 88.0]
NA_THUMB_REGION = [-170.0, 8.0, -40.0, 82.0]
CONUS_THUMB_REGION = [-127.0, 22.0, -65.0, 50.0]
# Northeast-only domain (excludes VA/NC by southern boundary; caps near tip of Maine)
NE_THUMB_REGION = [-82.5, 39.2, -66.0, 47.6]

# Boundaries overlays
COUNTRIES = ee.FeatureCollection('USDOS/LSIB_SIMPLE/2017')
US_STATES = ee.FeatureCollection('TIGER/2018/States')
NE_STATE_NAMES = [
    'Connecticut', 'Delaware', 'Maine', 'Maryland', 'Massachusetts',
    'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania',
    'Rhode Island', 'Vermont'
]
NE_STATES = US_STATES.filter(ee.Filter.inList('NAME', NE_STATE_NAMES))
NE_EXCLUDED_STATES = US_STATES.filter(ee.Filter.inList('NAME', ['Virginia', 'North Carolina']))


def ts():
    return time.strftime('%Y-%m-%d %H:%M:%S')

# Forecast hour controls
hours_csv = os.environ.get('HOURS_CSV')
hours_step_env = os.environ.get('HOURS_STEP')
hours_max_env = os.environ.get('HOURS_MAX')
hours_limit_env = os.environ.get('HOURS_LIMIT')
event_name = (os.environ.get('GITHUB_EVENT_NAME') or '').lower()
fast_render_env = os.environ.get('FAST_RENDER')
run_nh_z500a_env = os.environ.get('WN2_RUN_NH_Z500A')
run_na_z500a_env = os.environ.get('WN2_RUN_NA_Z500A')
run_conus_mslp_ptype_env = os.environ.get('WN2_RUN_CONUS_MSLP_PTYPE')
run_ne_mslp_ptype_env = os.environ.get('WN2_RUN_NE_MSLP_PTYPE')
run_conus_vort500_env = os.environ.get('WN2_RUN_CONUS_VORT500')
run_conus_snow_accum_env = os.environ.get('WN2_RUN_CONUS_SNOW_ACCUM')
run_ne_snow_accum_env = os.environ.get('WN2_RUN_NE_SNOW_ACCUM')


def _env_flag(raw, default=False):
    if raw is None:
        return default
    return str(raw).strip().lower() in ('1', 'true', 'yes', 'on')


def _select_product_flag(raw, default=True):
    if raw is None:
        return default
    return _env_flag(raw, default=default)


FAST_RENDER = _env_flag(fast_render_env, default=(event_name == 'schedule'))
if FAST_RENDER:
    ANOMALY_DIMS = '980x720'
    CONUS_DIMS = '1200x860'
    NE_DIMS = '1100x900'
    PTYPE_CONUS_DIMS = '1320x960'
    PTYPE_NE_DIMS = '1200x980'
    SNOW_CONUS_DIMS = '1320x960'
    SNOW_NE_DIMS = '1200x980'
    NH_SOURCE_DIMS = '1800x360'
    NH_POLAR_DIMS = 920
else:
    ANOMALY_DIMS = '1200x880'
    CONUS_DIMS = '1400x1000'
    NE_DIMS = '1200x980'
    PTYPE_CONUS_DIMS = '1600x1140'
    PTYPE_NE_DIMS = '1400x1120'
    SNOW_CONUS_DIMS = '1600x1140'
    SNOW_NE_DIMS = '1400x1120'
    NH_SOURCE_DIMS = '2200x440'
    NH_POLAR_DIMS = 1080

workers_env = os.environ.get('EXPORT_WORKERS')
try:
    EXPORT_WORKERS = int(workers_env) if workers_env else (2 if FAST_RENDER else 2)
except ValueError:
    EXPORT_WORKERS = 2
EXPORT_WORKERS = max(1, min(4, EXPORT_WORKERS))
print(f'[{ts()}] Render profile: fast={FAST_RENDER}, workers={EXPORT_WORKERS}, dims={ANOMALY_DIMS}/{CONUS_DIMS}.')

ANOMALY_PALETTE = [
    '#6a00a8', '#9c4dcc', '#5e60ce', '#2f80ed', '#7dcfff',
    '#f7f7f7',
    '#ffe08a', '#ffad5a', '#ff6b3a', '#d7301f', '#7f0000'
]
VORTICITY_PALETTE = ['#f5ee00', '#f4c236', '#ee8c4a', '#d35a75', '#a03ca0', '#5f209f']
RAIN_RATE_PALETTE = ['#a9ee80', '#7ad35a', '#4eb744', '#2f9637', '#f7ea00', '#ffbf00', '#ff8a00', '#ff4200', '#b70000', '#c21cff']
SNOW_RATE_PALETTE = ['#0a1f6f', '#0d2f8f', '#1448b1', '#1f66cc', '#2d84df', '#45a6ef', '#63c2ff']
FRZR_RATE_PALETTE = ['#ffe5ef', '#ffc4da', '#f78fb9', '#f06292', '#d81b60', '#ad1457', '#880e4f']
SLEET_RATE_PALETTE = ['#f0d9ff', '#e1bee7', '#ce93d8', '#ab47bc', '#8e24aa', '#6a1b9a']
SNOW_ACCUM_PALETTE = [
    '#f7fcff', '#e8f5ff',
    '#d7edff', '#a9d5ff', '#78b7ff', '#4b8de6', '#2a5fbf',  # 1-5 blue
    '#d9c8ff', '#b596ff', '#9164e8', '#7038c8', '#5420a8',  # 6-10 purple
    '#f9bddf', '#f58bc8', '#ec56b0', '#d92f95', '#b81479',  # 10-14 pink
    '#d7ffff', '#a8f7f7', '#73e8ee', '#3fd8e1', '#12c7d4', '#00b7c5',  # 14-24 cyan
    '#b8f5a8', '#8fe083', '#5fca57', '#35ae34', '#1f8e28', '#156d1f'   # 25-30 green
]

PRODUCT_OPTIONS = [
    ('nh_z500a', 'NH 500mb Height Anomaly', 'nh_z500a_*.jpg', run_nh_z500a_env),
    ('na_z500a', 'North America 500mb Height Anomaly', 'na_z500a_*.jpg', run_na_z500a_env),
    ('conus_mslp_ptype', 'CONUS MSLP + P-Type', 'conus_mslp_ptype_*.jpg', run_conus_mslp_ptype_env),
    ('ne_mslp_ptype', 'Northeast MSLP + P-Type', 'ne_mslp_ptype_*.jpg', run_ne_mslp_ptype_env),
    ('conus_vort500', 'CONUS 500mb Vorticity', 'conus_vort500_*.jpg', run_conus_vort500_env),
    ('conus_snow_accum', 'CONUS Snowfall Accumulation', 'conus_snow_accum_*.jpg', run_conus_snow_accum_env),
    ('ne_snow_accum', 'Northeast Snowfall Accumulation', 'ne_snow_accum_*.jpg', run_ne_snow_accum_env),
]

ENABLED_PRODUCTS = []
for key, label, pattern, raw_flag in PRODUCT_OPTIONS:
    enabled = _select_product_flag(raw_flag, default=True)
    if enabled:
        ENABLED_PRODUCTS.append((key, label, pattern))

if not ENABLED_PRODUCTS:
    raise ValueError(
        'No products selected. Enable at least one WN2_RUN_* product flag or select a checkbox in workflow_dispatch.'
    )

print(f'[{ts()}] Enabled products: {[k for k, _, _ in ENABLED_PRODUCTS]}')


def cleanup_old_products():
    stale_patterns = [
        'z500a_*.jpg',
        'nh_z500a_*.jpg',
        'na_z500a_*.jpg',
        'conus_mslp_ptype_*.jpg',
        'ne_mslp_ptype_*.jpg',
        'conus_vort500_*.jpg',
        'conus_snow_accum_*.jpg',
        'ne_snow_accum_*.jpg',
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
    last_error = None
    for attempt in range(1, 4):
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
            return
        except Exception as e:
            last_error = e
            msg = str(e)
            transient = (
                isinstance(e, (requests.ConnectionError, requests.Timeout))
                or 'RemoteDisconnected' in msg
                or 'Connection aborted' in msg
                or 'Read timed out' in msg
            )
            if transient and attempt < 3:
                wait_s = attempt * 3
                print(f'[{ts()}] Retry {attempt}/2 for {out_path} after transient error: {msg}')
                time.sleep(wait_s)
                continue
            print(f'[{ts()}] Thumbnail download error for {out_path}: {e}')
            raise
    if last_error is not None:
        raise last_error


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


def _parse_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _infer_available_hours(run_collection):
    raw_hours = run_collection.aggregate_array('forecast_hour').getInfo()
    parsed = sorted({h for h in (_parse_int(v) for v in raw_hours) if h is not None and h >= 0})
    if not parsed:
        raise ValueError('No numeric forecast_hour values found in latest run.')
    return parsed


def _select_hours(available_hours):
    explicit = None
    if hours_csv:
        explicit = sorted({int(x.strip()) for x in hours_csv.split(',') if x.strip()})
        if not explicit:
            raise ValueError('HOURS_CSV was set but no valid hour values were parsed.')
        print(f'[{ts()}] HOURS override from HOURS_CSV: {explicit}')
        selected = explicit
    else:
        deltas = sorted({b - a for a, b in zip(available_hours, available_hours[1:]) if (b - a) > 0})
        has_3h = 3 in deltas
        has_6h = 6 in deltas

        preferred_step = _parse_int(hours_step_env) if hours_step_env else None
        if preferred_step is not None and preferred_step <= 0:
            preferred_step = None

        if preferred_step is None:
            step = 3 if has_3h else 6 if has_6h else (deltas[0] if deltas else 6)
        else:
            step = preferred_step
            if step == 3 and not has_3h and has_6h:
                step = 6
            elif step == 6 and not has_6h and has_3h:
                step = 3
            elif step not in deltas and deltas:
                step = deltas[0]

        start_hour = available_hours[0]
        selected = [h for h in available_hours if (h - start_hour) % step == 0]
        if not selected:
            selected = available_hours

    max_hour = _parse_int(hours_max_env) if hours_max_env else None
    if max_hour is not None:
        bounded = [h for h in selected if h <= max_hour]
        if bounded:
            selected = bounded
        else:
            raise ValueError(f'HOURS_MAX={max_hour} filtered out all selected hours: {selected}')

    limit_count = _parse_int(hours_limit_env) if hours_limit_env else None
    if limit_count is not None and limit_count > 0:
        selected = selected[:limit_count]

    if explicit is None:
        note_parts = []
        if max_hour is not None:
            note_parts.append(f'max={max_hour}')
        if limit_count is not None and limit_count > 0:
            note_parts.append(f'limit={limit_count}')
        notes = f" ({', '.join(note_parts)})" if note_parts else ''
        print(
            f'[{ts()}] Auto HOURS: step={step}h, count={len(selected)}, '
            f'range={selected[0]}..{selected[-1]} (available range {available_hours[0]}..{available_hours[-1]}){notes}.'
        )
    else:
        note_parts = []
        if max_hour is not None:
            note_parts.append(f'max={max_hour}')
        if limit_count is not None and limit_count > 0:
            note_parts.append(f'limit={limit_count}')
        if note_parts:
            print(f"[{ts()}] HOURS post-filters: {', '.join(note_parts)} -> {selected}")
    return selected


AVAILABLE_HOURS = _infer_available_hours(latest_start_collection)
HOURS = _select_hours(AVAILABLE_HOURS)

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


def border_overlay(include_states=False, state_names=None):
    country_lines = ee.Image().byte().paint(COUNTRIES, 1, 1).selfMask().visualize(palette=['#333333'])
    if include_states:
        state_fc = US_STATES if not state_names else US_STATES.filter(ee.Filter.inList('NAME', state_names))
        state_lines = ee.Image().byte().paint(state_fc, 1, 1).selfMask().visualize(palette=['#6b4a2c'])
        return ee.ImageCollection([country_lines, state_lines]).mosaic()
    return country_lines


def basemap_overlay(region_geom, land_color='#ececec', ocean_color='#cfe0ea', land_fc=None):
    ocean = ee.Image.constant(1).clip(region_geom).visualize(palette=[ocean_color], opacity=1.0)
    land_features = land_fc if land_fc is not None else COUNTRIES
    land_mask = ee.Image().byte().paint(land_features, 1, 1).clip(region_geom).selfMask()
    land = land_mask.visualize(palette=[land_color], opacity=1.0)
    return ee.ImageCollection([ocean, land]).mosaic()


def pseudo_z500_anomaly_m(height_dam, radius_px=24):
    # Remove the broad background field so synoptic anomalies stand out.
    broad = height_dam.resample('bilinear').focalMean(radius=radius_px, kernelType='circle', units='pixels')
    return height_dam.subtract(broad).multiply(10)


def shrink_dimensions(dimensions):
    if isinstance(dimensions, int):
        return max(500, int(dimensions * 0.82))
    if isinstance(dimensions, str) and 'x' in dimensions:
        w_str, h_str = dimensions.lower().split('x', 1)
        try:
            w = int(w_str)
            h = int(h_str)
            return f'{max(500, int(w * 0.82))}x{max(400, int(h * 0.82))}'
        except ValueError:
            return dimensions
    return dimensions


def split_nh_dimensions(source_dims):
    if isinstance(source_dims, str) and 'x' in source_dims:
        w_str, h_str = source_dims.lower().split('x', 1)
        try:
            w = int(w_str)
            h = int(h_str)
            return f'{max(420, w // 2)}x{max(280, h)}'
        except ValueError:
            return '1100x440'
    return '1100x440'


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


def _hex_to_rgb(color_hex):
    color_hex = color_hex.strip().lstrip('#')
    if len(color_hex) != 6:
        return (0, 0, 0)
    return tuple(int(color_hex[i:i + 2], 16) for i in (0, 2, 4))


def _interp_palette_color(palette, t):
    if not palette:
        return (0, 0, 0)
    if len(palette) == 1:
        return _hex_to_rgb(palette[0])
    t = max(0.0, min(1.0, t))
    pos = t * (len(palette) - 1)
    idx = int(math.floor(pos))
    frac = pos - idx
    if idx >= len(palette) - 1:
        return _hex_to_rgb(palette[-1])
    c0 = _hex_to_rgb(palette[idx])
    c1 = _hex_to_rgb(palette[idx + 1])
    return (
        int(round(c0[0] + (c1[0] - c0[0]) * frac)),
        int(round(c0[1] + (c1[1] - c0[1]) * frac)),
        int(round(c0[2] + (c1[2] - c0[2]) * frac)),
    )


def _draw_gradient_bar(draw, x, y, w, h, palette):
    if w < 2 or h < 2:
        return
    for i in range(w):
        frac = i / float(max(1, w - 1))
        draw.line([(x + i, y), (x + i, y + h)], fill=_interp_palette_color(palette, frac), width=1)
    draw.rectangle((x, y, x + w, y + h), outline=(50, 50, 50), width=1)


def _draw_tapered_gradient_bar(draw, x, y, w, h, palette):
    taper = max(8, min(18, w // 12))
    inner_x = x + taper
    inner_w = max(4, w - 2 * taper)
    for i in range(inner_w):
        frac = i / float(max(1, inner_w - 1))
        draw.line([(inner_x + i, y), (inner_x + i, y + h)], fill=_interp_palette_color(palette, frac), width=1)
    for i in range(taper):
        frac = i / float(max(1, taper - 1))
        color_l = _interp_palette_color(palette, 0.0)
        color_r = _interp_palette_color(palette, 1.0)
        yy0 = y + int(round(h * 0.5 * frac))
        yy1 = y + h - int(round(h * 0.5 * frac))
        draw.line((x + i, yy0, x + i, yy1), fill=color_l, width=1)
        draw.line((x + w - 1 - i, yy0, x + w - 1 - i, yy1), fill=color_r, width=1)
    draw.rectangle((x, y, x + w, y + h), outline=(50, 50, 50), width=1)


def _text_size(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    return (bbox[2] - bbox[0], bbox[3] - bbox[1])


def _fit_font(draw, text, start_size, min_size, bold=False, max_width=1000):
    size = start_size
    while size >= min_size:
        font = load_font(size, bold=bold)
        tw, _ = _text_size(draw, text, font)
        if tw <= max_width:
            return font
        size -= 1
    return load_font(min_size, bold=bold)


def _format_tick_label(value):
    if isinstance(value, (int, float)) and value != 0 and abs(value) < 1:
        s = f'{value:g}'
        if s.startswith('-0'):
            return '-' + s[2:]
        if s.startswith('0'):
            return s[1:]
        return s
    return f'{value:g}'


def _draw_ticks(draw, x, y, w, h, values, vmin, vmax, font):
    if vmax <= vmin:
        return
    for value in values:
        frac = (value - vmin) / float(vmax - vmin)
        if frac < 0 or frac > 1:
            continue
        tx = int(round(x + frac * w))
        draw.line((tx, y + h, tx, y + h + 5), fill=(40, 40, 40), width=1)
        label = _format_tick_label(value)
        tw, _ = _text_size(draw, label, font)
        draw.text((tx - tw // 2, y + h + 7), label, fill=(30, 30, 30), font=font)


def _draw_panel(draw, x0, y0, x1, y1, fill=(242, 242, 242), outline=(120, 120, 120), radius=10):
    draw.rounded_rectangle((x0, y0, x1, y1), radius=radius, fill=fill, outline=outline, width=2)


def _draw_segmented_gradient_bar(draw, x, y, w, h, segments, vmin, vmax):
    if w < 2 or h < 2 or vmax <= vmin:
        return
    for low, high, palette in segments:
        clamped_low = max(vmin, min(vmax, low))
        clamped_high = max(vmin, min(vmax, high))
        if clamped_high <= clamped_low:
            continue
        sx = x + int(round((clamped_low - vmin) / float(vmax - vmin) * w))
        ex = x + int(round((clamped_high - vmin) / float(vmax - vmin) * w))
        seg_w = max(1, ex - sx)
        for i in range(seg_w):
            frac = i / float(max(1, seg_w - 1))
            draw.line([(sx + i, y), (sx + i, y + h)], fill=_interp_palette_color(palette, frac), width=1)
    draw.rectangle((x, y, x + w, y + h), outline=(50, 50, 50), width=1)


def _draw_legend(draw, product_key, width, y):
    label_font = load_font(22 if width >= 1200 else 18, bold=False)
    tick_font = load_font(16 if width >= 1200 else 14, bold=False)
    x_pad = 58 if width >= 1200 else 36
    bar_x = x_pad
    bar_w = width - 2 * x_pad
    bar_y = y + 26
    bar_h = 22

    if product_key in ('nh_z500a', 'na_z500a'):
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '500-hPa Height Anomaly (m)', fill=(22, 22, 22), font=label_font)
        _draw_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, ANOMALY_PALETTE)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [-100, -70, -40, -20, 0, 20, 40, 70, 100], -100, 100, tick_font)
        return

    if product_key == 'conus_vort500':
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '500-hPa Relative Vorticity (x10^-5 s^-1)', fill=(22, 22, 22), font=label_font)
        _draw_tapered_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, VORTICITY_PALETTE)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48], 4, 50, tick_font)
        return

    if product_key in ('conus_snow_accum', 'ne_snow_accum'):
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), 'Accumulated Snowfall Total (in, 10:1 ratio)', fill=(22, 22, 22), font=label_font)
        snow_segments = [
            (0.1, 1.0, ['#f7fcff', '#e8f5ff']),
            (1.0, 5.0, ['#d7edff', '#a9d5ff', '#78b7ff', '#4b8de6', '#2a5fbf']),
            (5.0, 10.0, ['#d9c8ff', '#b596ff', '#9164e8', '#7038c8', '#5420a8']),
            (10.0, 14.0, ['#f9bddf', '#f58bc8', '#ec56b0', '#d92f95', '#b81479']),
            (14.0, 24.0, ['#d7ffff', '#a8f7f7', '#73e8ee', '#3fd8e1', '#12c7d4', '#00b7c5']),
            (24.0, 30.0, ['#b8f5a8', '#8fe083', '#5fca57', '#35ae34', '#1f8e28', '#156d1f']),
        ]
        _draw_segmented_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, snow_segments, 0.1, 30.0)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [0.1, 1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 18, 24, 25, 27, 30], 0.1, 30.0, tick_font)
        return

    if product_key in ('conus_mslp_ptype', 'ne_mslp_ptype'):
        draw.text((bar_x, y + 2), 'Precip Rate (mm/hr) by Type', fill=(22, 22, 22), font=label_font)
        gap = 16 if width >= 1200 else 10
        slot_w = int((bar_w - 3 * gap) / 4)
        panel_h = 96
        labels = [
            ('Rain', RAIN_RATE_PALETTE),
            ('Snow', SNOW_RATE_PALETTE),
            ('Freezing Rain', FRZR_RATE_PALETTE),
            ('Sleet', SLEET_RATE_PALETTE),
        ]
        for i, (name, palette) in enumerate(labels):
            sx = bar_x + i * (slot_w + gap)
            panel_x0 = sx - 6
            panel_y0 = y + 28
            panel_x1 = sx + slot_w + 6
            panel_y1 = panel_y0 + panel_h
            _draw_panel(draw, panel_x0, panel_y0, panel_x1, panel_y1, fill=(242, 242, 242), outline=(130, 130, 130), radius=8)
            draw.text((sx + 2, y + 34), name, fill=(24, 24, 24), font=tick_font)
            gradient_y = y + 58
            _draw_gradient_bar(draw, sx, gradient_y, slot_w, bar_h, palette)
            _draw_ticks(draw, sx, gradient_y, slot_w, bar_h, [0.1, 0.3, 1, 3, 6, 11, 24, 38], 0.1, 38.0, tick_font)


def _lonlat_to_image_xy(lon, lat, region, width, height):
    if lon is None or lat is None or not region or len(region) != 4:
        return None
    west, south, east, north = region
    if north <= south:
        return None

    lon_f = float(lon)
    lat_f = float(lat)
    lat_f = max(south, min(north, lat_f))

    if east > west:
        span = east - west
        lon_norm = (lon_f - west) / span
    else:
        span = (east + 360.0) - west
        wrapped = lon_f
        if wrapped < west:
            wrapped += 360.0
        lon_norm = (wrapped - west) / span

    lon_norm = max(0.0, min(1.0, lon_norm))
    y_norm = (north - lat_f) / (north - south)
    y_norm = max(0.0, min(1.0, y_norm))

    x = int(round(lon_norm * (width - 1)))
    y = int(round(y_norm * (height - 1)))
    return x, y


def annotate_map_file(out_file, product_key, hour, map_region=None, low_center=None):
    from PIL import Image, ImageDraw

    product_titles = {
        'nh_z500a': 'WN2 0.25 deg | 500-hPa Geopotential Height (dam) & Anomaly (m) | Northern Hemisphere',
        'na_z500a': 'WN2 0.25 deg | 500-hPa Geopotential Height (dam) & Anomaly (m) | North America',
        'conus_mslp_ptype': 'WN2 0.25 deg | MSLP (hPa) + Precip Type | CONUS',
        'ne_mslp_ptype': 'WN2 0.25 deg | MSLP (hPa) + Precip Type | Northeast',
        'conus_vort500': 'WN2 0.25 deg | 500-hPa Relative Vorticity + 500-hPa Height (dam) | CONUS',
        'conus_snow_accum': 'WN2 0.25 deg | Accumulated Snowfall (in, 10:1) + MSLP (hPa) | CONUS',
        'ne_snow_accum': 'WN2 0.25 deg | Accumulated Snowfall (in, 10:1) + MSLP (hPa) | Northeast',
    }
    title = product_titles.get(product_key, product_key)
    init_text, valid_text = format_map_times(hour)
    subtitle = f'Init: {init_text} | Hour: [{hour:03d}] | Valid: {valid_text}'

    with Image.open(out_file) as src:
        img = src.convert('RGB')
        if map_region is not None and low_center:
            marker_xy = _lonlat_to_image_xy(
                low_center.get('lon'),
                low_center.get('lat'),
                map_region,
                img.width,
                img.height,
            )
            if marker_xy is not None:
                marker_draw = ImageDraw.Draw(img)
                marker_font = load_font(60 if img.width >= 1300 else 52, bold=True)
                value_font = load_font(28 if img.width >= 1300 else 24, bold=True)
                x, y = marker_xy
                l_tw, l_th = _text_size(marker_draw, 'L', marker_font)
                l_x = int(round(x - l_tw / 2))
                l_y = int(round(y - l_th / 2))
                l_x = max(4, min(img.width - l_tw - 4, l_x))
                l_y = max(4, min(img.height - l_th - 4, l_y))
                try:
                    marker_draw.text((l_x, l_y), 'L', fill=(214, 28, 28), font=marker_font, stroke_width=2, stroke_fill=(20, 20, 20))
                except TypeError:
                    marker_draw.text((l_x, l_y), 'L', fill=(214, 28, 28), font=marker_font)

                mb = low_center.get('mb')
                if mb is not None:
                    mb_text = f'{int(round(float(mb)))} mb'
                    mb_tw, mb_th = _text_size(marker_draw, mb_text, value_font)
                    mb_x = l_x + l_tw + 8
                    mb_y = l_y + max(0, (l_th - mb_th) // 2)
                    if mb_x + mb_tw > img.width - 4:
                        mb_x = max(4, l_x - mb_tw - 8)
                    mb_x = max(4, min(img.width - mb_tw - 4, mb_x))
                    mb_y = max(4, min(img.height - mb_th - 4, mb_y))
                    try:
                        marker_draw.text((mb_x, mb_y), mb_text, fill=(214, 28, 28), font=value_font, stroke_width=2, stroke_fill=(20, 20, 20))
                    except TypeError:
                        marker_draw.text((mb_x, mb_y), mb_text, fill=(214, 28, 28), font=value_font)

        legend_h = 0
        if product_key in ('conus_mslp_ptype', 'ne_mslp_ptype'):
            legend_h = 180
        elif product_key in ('nh_z500a', 'na_z500a', 'conus_vort500', 'conus_snow_accum', 'ne_snow_accum'):
            legend_h = 96

        header_h = 78
        footer_h = 22
        canvas = Image.new('RGB', (img.width, img.height + header_h + legend_h + footer_h), color=(236, 236, 236))
        canvas.paste(img, (0, header_h))
        draw = ImageDraw.Draw(canvas)

        title_font = load_font(30 if img.width >= 1300 else 26, bold=True)
        subtitle_font = load_font(22 if img.width >= 1300 else 19, bold=False)
        footer_font = load_font(15 if img.width >= 1300 else 13, bold=False)
        max_text_w = img.width - 24
        title_font = _fit_font(
            draw,
            title,
            start_size=(30 if img.width >= 1300 else 26),
            min_size=(18 if img.width >= 1300 else 16),
            bold=True,
            max_width=max_text_w,
        )
        subtitle_font = _fit_font(
            draw,
            subtitle,
            start_size=(22 if img.width >= 1300 else 19),
            min_size=(14 if img.width >= 1300 else 13),
            bold=False,
            max_width=max_text_w,
        )

        draw.text((12, 10), title, fill=(20, 20, 20), font=title_font)
        draw.text((12, 44), subtitle, fill=(25, 25, 25), font=subtitle_font)
        draw.rectangle((0, header_h, img.width - 1, header_h + img.height - 1), outline=(32, 32, 32), width=2)

        if legend_h > 0:
            _draw_legend(draw, product_key, img.width, header_h + img.height + 2)

        footer_text = f'Run: {run_date} | Source: WeatherNext2 (Earth Engine)'
        draw.text((12, img.height + header_h + legend_h + 2), footer_text, fill=(35, 35, 35), font=footer_font)
        canvas.save(out_file, format='JPEG', quality=97, subsampling=0)


def remap_nh_to_polar(out_file, lon0=NH_LON0, lat_min=20.0, lat_max=89.0):
    from PIL import Image, ImageDraw

    with Image.open(out_file) as src:
        src_img = src.convert('RGB')
    sw, sh = src_img.size
    src_px = src_img.load()

    out_size = NH_POLAR_DIMS
    out_img = Image.new('RGB', (out_size, out_size), color=(214, 214, 214))
    out_px = out_img.load()

    cx = (out_size - 1) / 2.0
    cy = (out_size - 1) / 2.0
    radius = out_size * 0.48

    for y in range(out_size):
        dy = (y - cy) / radius
        for x in range(out_size):
            dx = (x - cx) / radius
            r = math.hypot(dx, dy)
            if r > 1.0:
                continue

            lat = 90.0 - r * (90.0 - lat_min)
            lon = lon0 + math.degrees(math.atan2(dx, -dy))
            lon = ((lon + 180.0) % 360.0) - 180.0

            sx = int(round((lon + 180.0) / 360.0 * (sw - 1)))
            sy = int(round((lat_max - lat) / (lat_max - lat_min) * (sh - 1)))
            if sy < 0:
                sy = 0
            elif sy >= sh:
                sy = sh - 1

            out_px[x, y] = src_px[sx, sy]

    draw = ImageDraw.Draw(out_img)
    for lat_line in [30, 40, 50, 60, 70, 80]:
        rr = (90.0 - lat_line) / (90.0 - lat_min) * radius
        draw.ellipse((cx - rr, cy - rr, cx + rr, cy + rr), outline=(120, 120, 120), width=1)
    for lon_deg in range(0, 360, 30):
        ang = math.radians(lon_deg)
        x2 = cx + radius * math.sin(ang)
        y2 = cy - radius * math.cos(ang)
        draw.line((cx, cy, x2, y2), fill=(120, 120, 120), width=1)
    draw.ellipse((cx - radius, cy - radius, cx + radius, cy + radius), outline=(35, 35, 35), width=3)

    out_img.save(out_file, format='JPEG', quality=97, subsampling=0)


def stitch_horizontal(left_file, right_file, out_file):
    from PIL import Image

    with Image.open(left_file) as left_src:
        left = left_src.convert('RGB')
    with Image.open(right_file) as right_src:
        right = right_src.convert('RGB')

    height = min(left.height, right.height)
    if left.height != height:
        left = left.resize((left.width, height))
    if right.height != height:
        right = right.resize((right.width, height))

    stitched = Image.new('RGB', (left.width + right.width, height))
    stitched.paste(left, (0, 0))
    stitched.paste(right, (left.width, 0))
    stitched.save(out_file, format='JPEG', quality=97, subsampling=0)


def export_composite(composite, out_file, region, dimensions=1600, scale=None, crs=None):
    print(f'[{ts()}] Exporting {out_file}...')
    t0 = time.time()
    params = {
        'region': region,
        'format': 'jpg',
    }
    if crs is not None:
        params['crs'] = crs
    elif scale is not None:
        params['crs'] = TARGET_CRS
    if scale is not None:
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
    if prefix == 'nh_z500a':
        region_geom = WORLD_REGION
        export_region = NH_SOURCE_REGION
        export_crs = None
        map_dims = NH_SOURCE_DIMS
    else:
        region_geom = ee.Geometry.Rectangle(region, geodesic=False)
        export_region = region
        export_crs = None
        map_dims = ANOMALY_DIMS

    anomaly_radius = 30 if prefix == 'nh_z500a' else 20
    anomaly_min = -100 if prefix == 'nh_z500a' else -100
    anomaly_max = 100 if prefix == 'nh_z500a' else 100
    forecast_height_dam = img.select(WN2_Z500_BAND).divide(9.80665).divide(10)
    if prefix != 'nh_z500a':
        forecast_height_dam = forecast_height_dam.clip(region_geom)
    anomaly_m = pseudo_z500_anomaly_m(forecast_height_dam, radius_px=anomaly_radius)

    anomaly_layer = anomaly_m.visualize(
        min=anomaly_min,
        max=anomaly_max,
        palette=ANOMALY_PALETTE,
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
    if prefix == 'nh_z500a':
        west_file = f'{OUTPUT}/_tmp_nh_w_{h:03d}.jpg'
        east_file = f'{OUTPUT}/_tmp_nh_e_{h:03d}.jpg'
        split_dims = split_nh_dimensions(NH_SOURCE_DIMS)
        export_composite(composite.clip(NH_W), west_file, NH_W_BOUNDS, dimensions=split_dims)
        export_composite(composite.clip(NH_E), east_file, NH_E_BOUNDS, dimensions=split_dims)
        stitch_horizontal(west_file, east_file, out_file)
        os.remove(west_file)
        os.remove(east_file)
        remap_nh_to_polar(out_file, lon0=NH_LON0)
        annotate_map_file(out_file, prefix, h)
        return

    export_composite(
        composite,
        out_file,
        export_region,
        dimensions=map_dims,
        crs=export_crs,
    )
    annotate_map_file(out_file, prefix, h)


def derive_precip_phase(img, region_geom):
    precip_6h_mm = img.select(WN2_PRECIP_6H_BAND).multiply(1000).clip(region_geom)
    precip_rate = precip_6h_mm.divide(6)  # mm/hr
    precip_rate_sm = precip_rate.focalMean(2, 'circle', 'pixels')
    precip_mask = precip_rate_sm.gt(0.35)

    t2c = img.select(WN2_T2M_BAND).subtract(273.15).clip(region_geom)
    t850c = img.select(WN2_T850_BAND).subtract(273.15).clip(region_geom)
    t700c = img.select(WN2_T700_BAND).subtract(273.15).clip(region_geom)

    snow = precip_mask.And(t2c.lte(1)).And(t850c.lte(-1)).And(t700c.lte(-2))
    freezing_rain = precip_mask.And(t2c.lte(0)).And(t850c.gt(1)).And(t700c.gt(-2))
    sleet = precip_mask.And(t2c.lte(0)).And(t850c.gt(0)).And(t700c.lte(-2)).And(freezing_rain.Not())
    rain = precip_mask.And(snow.Not()).And(freezing_rain.Not()).And(sleet.Not())
    return precip_rate_sm, precip_6h_mm, rain, snow, freezing_rain, sleet


def smooth_precip_type_masks(precip_rate, rain, snow, freezing_rain, sleet):
    precip_mask = precip_rate.gt(0.25)
    ptype = ee.Image.constant(0).updateMask(precip_mask)
    ptype = ptype.where(snow, 1)
    ptype = ptype.where(freezing_rain, 2)
    ptype = ptype.where(sleet, 3)
    ptype_sm = ptype.focalMode(1, 'circle', 'pixels').updateMask(precip_mask)
    rain_sm = ptype_sm.eq(0).And(precip_mask)
    snow_sm = ptype_sm.eq(1).And(precip_mask)
    frz_sm = ptype_sm.eq(2).And(precip_mask)
    sleet_sm = ptype_sm.eq(3).And(precip_mask)
    return rain_sm, snow_sm, frz_sm, sleet_sm


def find_low_center(mslp_hpa, region_geom, scale_m=50000):
    try:
        min_stats = mslp_hpa.reduceRegion(
            reducer=ee.Reducer.min(),
            geometry=region_geom,
            scale=scale_m,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=4,
        ).getInfo() or {}
        min_val = min_stats.get(WN2_MSLP_BAND)
        if min_val is None:
            return None

        min_val = float(min_val)
        min_mask = mslp_hpa.lte(min_val + 0.08).selfMask()
        lonlat = ee.Image.pixelLonLat().updateMask(min_mask).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region_geom,
            scale=scale_m,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=4,
        ).getInfo() or {}
        lon = lonlat.get('longitude')
        lat = lonlat.get('latitude')
        if lon is None or lat is None:
            return {'mb': int(round(min_val))}
        return {
            'lon': float(lon),
            'lat': float(lat),
            'mb': int(round(min_val)),
        }
    except Exception as e:
        print(f'[{ts()}] Low-center detection skipped: {e}')
        return None


def snow_increment_cm(img, region_geom):
    precip_rate_sm, precip_6h_mm, _, snow, _, _ = derive_precip_phase(img, region_geom)
    robust_snow = snow.And(precip_rate_sm.gt(0.2))
    return precip_6h_mm.updateMask(robust_snow).unmask(0).rename('snow_cm_6h')


def range_gradient_layer(field, low, high, palette, include_high=False):
    span = max(high - low, 0.001)
    norm = field.subtract(low).divide(span).clamp(0, 1)
    if include_high:
        mask = field.gte(low).And(field.lte(high))
    else:
        mask = field.gte(low).And(field.lt(high))
    return norm.updateMask(mask).visualize(min=0, max=1, palette=palette)


def snow_accum_layer(snow_total_in):
    return ee.ImageCollection([
        range_gradient_layer(snow_total_in, 0.1, 1.0, ['#f7fcff', '#e8f5ff']),
        range_gradient_layer(snow_total_in, 1.0, 5.0, ['#d7edff', '#a9d5ff', '#78b7ff', '#4b8de6', '#2a5fbf']),
        range_gradient_layer(snow_total_in, 5.0, 10.0, ['#d9c8ff', '#b596ff', '#9164e8', '#7038c8', '#5420a8']),
        range_gradient_layer(snow_total_in, 10.0, 14.0, ['#f9bddf', '#f58bc8', '#ec56b0', '#d92f95', '#b81479']),
        range_gradient_layer(snow_total_in, 14.0, 24.0, ['#d7ffff', '#a8f7f7', '#73e8ee', '#3fd8e1', '#12c7d4', '#00b7c5']),
        range_gradient_layer(snow_total_in, 24.0, 30.0, ['#b8f5a8', '#8fe083', '#5fca57', '#35ae34', '#1f8e28', '#156d1f'], include_high=True),
        snow_total_in.gt(30.0).selfMask().visualize(palette=['#156d1f']),
    ]).mosaic()


def generate_mslp_ptype_map(img, h, region=CONUS_THUMB_REGION, key='conus_mslp_ptype'):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    is_ne = key.startswith('ne_')
    state_names = NE_STATE_NAMES if is_ne else None
    land_fc = NE_STATES if is_ne else None
    work_geom = region_geom.difference(NE_EXCLUDED_STATES.geometry(), maxError=1000) if is_ne else region_geom
    precip_rate, _, rain, snow, freezing_rain, sleet = derive_precip_phase(img, work_geom)
    rain_sm, snow_sm, frz_sm, sleet_sm = smooth_precip_type_masks(precip_rate, rain, snow, freezing_rain, sleet)
    precip_rate_vis = precip_rate.resample('bilinear').focalMean(1, 'circle', 'pixels')

    rain_layer = precip_rate_vis.updateMask(rain_sm).visualize(
        min=0.05, max=25,
        palette=RAIN_RATE_PALETTE,
    )
    snow_layer = precip_rate_vis.updateMask(snow_sm).visualize(
        min=0.05, max=25,
        palette=SNOW_RATE_PALETTE,
    )
    frz_layer = precip_rate_vis.updateMask(frz_sm).visualize(
        min=0.05, max=25,
        palette=FRZR_RATE_PALETTE,
    )
    sleet_layer = precip_rate_vis.updateMask(sleet_sm).visualize(
        min=0.05, max=25,
        palette=SLEET_RATE_PALETTE,
    )

    mslp_hpa = img.select(WN2_MSLP_BAND).divide(100).clip(work_geom)
    mslp_contours = contour_overlay(
        mslp_hpa,
        interval=3,
        color='#2a2a2a',
        opacity=0.9,
    )
    low_center = find_low_center(mslp_hpa, work_geom)

    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color='#eeeeee', ocean_color='#c7dbe6', land_fc=land_fc),
        rain_layer,
        snow_layer,
        frz_layer,
        sleet_layer,
        mslp_contours,
        border_overlay(include_states=True, state_names=state_names),
    ]).mosaic()

    out_file = f'{OUTPUT}/{key}_{h:03d}.jpg'
    dims = PTYPE_NE_DIMS if key.startswith('ne_') else PTYPE_CONUS_DIMS
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h, map_region=region, low_center=low_center)


def generate_snow_accum_map(img, h, running_snow_cm, region=CONUS_THUMB_REGION, key='conus_snow_accum'):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    is_ne = key.startswith('ne_')
    state_names = NE_STATE_NAMES if is_ne else None
    land_fc = NE_STATES if is_ne else None
    work_geom = region_geom.difference(NE_EXCLUDED_STATES.geometry(), maxError=1000) if is_ne else region_geom
    snow_total_in = running_snow_cm.divide(2.54).clip(work_geom)
    snow_total_vis = snow_total_in.resample('bilinear').focalMean(1, 'circle', 'pixels')
    snow_layer = snow_accum_layer(snow_total_vis)
    mslp_hpa = img.select(WN2_MSLP_BAND).divide(100).clip(work_geom)
    mslp_contours = contour_overlay(mslp_hpa, interval=3, color='#2f2f2f', opacity=0.9)
    z500_height_dam = img.select(WN2_Z500_BAND).divide(9.80665).divide(10).clip(work_geom)
    z500_contours = contour_overlay(z500_height_dam, interval=6, color='#4a4a4a', opacity=0.76)

    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color='#eeeeee', ocean_color='#c7dbe6', land_fc=land_fc),
        snow_layer,
        mslp_contours,
        z500_contours,
        border_overlay(include_states=True, state_names=state_names),
    ]).mosaic()

    out_file = f'{OUTPUT}/{key}_{h:03d}.jpg'
    dims = SNOW_NE_DIMS if key.startswith('ne_') else SNOW_CONUS_DIMS
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h)


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
        palette=VORTICITY_PALETTE,
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
product_patterns = [pattern for _, _, pattern in ENABLED_PRODUCTS]
running_snow_cm = ee.Image.constant(0).clip(CONUS_REGION).rename('snow_total_cm')
needs_snow_accum = any(k in ('conus_snow_accum', 'ne_snow_accum') for k, _, _ in ENABLED_PRODUCTS)

for h in HOURS:
    print(f'Generating Hour {h}...')
    img = get_hour_image(h)
    if needs_snow_accum:
        running_snow_cm = running_snow_cm.add(snow_increment_cm(img, CONUS_REGION)).rename('snow_total_cm')

    tasks = []
    enabled_keys = {k for k, _, _ in ENABLED_PRODUCTS}
    if 'nh_z500a' in enabled_keys:
        tasks.append(('nh_z500a', lambda i=img, hh=h: generate_z500_anomaly_map(i, hh, NH_THUMB_REGION, 'nh_z500a')))
    if 'na_z500a' in enabled_keys:
        tasks.append(('na_z500a', lambda i=img, hh=h: generate_z500_anomaly_map(i, hh, NA_THUMB_REGION, 'na_z500a')))
    if 'conus_mslp_ptype' in enabled_keys:
        tasks.append(('conus_mslp_ptype', lambda i=img, hh=h: generate_mslp_ptype_map(i, hh, CONUS_THUMB_REGION, 'conus_mslp_ptype')))
    if 'ne_mslp_ptype' in enabled_keys:
        tasks.append(('ne_mslp_ptype', lambda i=img, hh=h: generate_mslp_ptype_map(i, hh, NE_THUMB_REGION, 'ne_mslp_ptype')))
    if 'conus_vort500' in enabled_keys:
        tasks.append(('conus_vort500', lambda i=img, hh=h: generate_vort500_map(i, hh)))
    if 'conus_snow_accum' in enabled_keys:
        tasks.append(('conus_snow_accum', lambda i=img, hh=h, s=running_snow_cm: generate_snow_accum_map(i, hh, s, CONUS_THUMB_REGION, 'conus_snow_accum')))
    if 'ne_snow_accum' in enabled_keys:
        tasks.append(('ne_snow_accum', lambda i=img, hh=h, s=running_snow_cm: generate_snow_accum_map(i, hh, s, NE_THUMB_REGION, 'ne_snow_accum')))
    with ThreadPoolExecutor(max_workers=EXPORT_WORKERS) as pool:
        future_to_name = {pool.submit(fn): name for name, fn in tasks}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                future.result()
                successful_exports += 1
            except Exception as e:
                err_msg = str(e)
                print(f'[{ts()}] Hour {h} product {name}: FAILED - {err_msg}')
                failures.append((f'{h}:{name}', err_msg))
                if 'earthengine.thumbnails.create' in err_msg:
                    for pending in future_to_name:
                        if pending is not future:
                            pending.cancel()
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
product_options_html = '\n'.join(
    [f'                <option value="{key}">{label}</option>' for key, label, _ in ENABLED_PRODUCTS]
)
html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>WN2 Multi-Product Viewer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {{
            background:#1f1f1f;
            color:#efefef;
            font-family:system-ui, sans-serif;
            text-align:center;
            margin:0;
            padding-bottom:136px;
        }}
        .wrap {{ max-width:1240px; margin:0 auto; padding:14px 10px 18px; }}
        .map-wrap {{ background:#111; border:1px solid #4f4f4f; }}
        img {{ width:100%; height:auto; display:block; background:#111; }}
        button {{
            padding:10px 16px;
            font-size:16px;
            cursor:pointer;
            border:1px solid #666;
            background:#2d2d2d;
            color:#f1f1f1;
            border-radius:8px;
        }}
        select {{
            padding:8px 10px;
            font-size:15px;
            border-radius:8px;
            border:1px solid #666;
            background:#2a2a2a;
            color:#f1f1f1;
        }}
        #label {{
            display:inline-block;
            min-width:120px;
            font-weight:700;
            letter-spacing:0.03em;
        }}
        .bottom-controls {{
            position:fixed;
            left:0;
            right:0;
            bottom:0;
            background:#141414;
            border-top:1px solid #3c3c3c;
            padding:10px 10px 12px;
            box-shadow:0 -6px 16px rgba(0, 0, 0, 0.35);
        }}
        .row {{
            max-width:1240px;
            margin:0 auto;
            display:flex;
            align-items:center;
            justify-content:center;
            flex-wrap:wrap;
            gap:10px;
        }}
        #hourSlider {{
            width:min(960px, 94vw);
            height:34px;
            touch-action:pan-x;
        }}
        @media (max-width: 760px) {{
            h2 {{ font-size:20px; margin:10px 0; }}
            button {{ font-size:14px; padding:8px 12px; }}
            select {{ font-size:14px; }}
            #label {{ min-width:86px; }}
            body {{ padding-bottom:160px; }}
        }}
    </style>
</head>
<body>
    <div class="wrap">
        <h2>WeatherNext 2 Map Viewer: {run_date}</h2>
        <div class="map-wrap">
            <img id="map" src="" alt="WN2 map">
        </div>
    </div>

    <div class="bottom-controls">
        <div class="row">
            <label for="product">Map:</label>
            <select id="product">
{product_options_html}
            </select>
            <button id="prevBtn">Prev</button>
            <span id="label">Hour ---</span>
            <button id="nextBtn">Next</button>
        </div>
        <div class="row" style="margin-top:6px;">
            <input type="range" id="hourSlider" min="0" max="0" step="1" value="0">
        </div>
    </div>

    <script>
        const hours = {HOURS};
        let idx = 0;

        const mapEl = document.getElementById('map');
        const labelEl = document.getElementById('label');
        const productEl = document.getElementById('product');
        const sliderEl = document.getElementById('hourSlider');
        const prevBtn = document.getElementById('prevBtn');
        const nextBtn = document.getElementById('nextBtn');

        function render() {{
            if (!hours.length) {{
                mapEl.src = '';
                labelEl.innerText = 'No hours';
                return;
            }}
            const hour = hours[idx];
            const hourStr = hour.toString().padStart(3, '0');
            const product = productEl.value;
            mapEl.src = `${{product}}_${{hourStr}}.jpg`;
            labelEl.innerText = `Hour ${{hourStr}}`;
        }}

        function change(dir) {{
            if (!hours.length) return;
            idx = (idx + dir + hours.length) % hours.length;
            sliderEl.value = String(idx);
            render();
        }}

        sliderEl.addEventListener('input', () => {{
            idx = Number(sliderEl.value);
            render();
        }});
        productEl.addEventListener('change', render);
        prevBtn.addEventListener('click', () => change(-1));
        nextBtn.addEventListener('click', () => change(1));

        if (hours.length > 0) {{
            sliderEl.max = String(hours.length - 1);
            sliderEl.value = '0';
            render();
        }} else {{
            render();
        }}
    </script>
</body>
</html>
"""

with open(f'{OUTPUT}/index.html', 'w') as f:
    f.write(html)

