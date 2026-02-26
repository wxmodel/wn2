import ee
import os
import json
import time
import glob
import hashlib
import math
import shutil
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
CLIMO_ASSET = 'NASA/GSFC/MERRA/slv/2'
CLIMO_H500_BAND = 'H500'
CLIMO_START_YEAR = 1991
CLIMO_END_YEAR = 2020
CLIMO_DOY_WINDOW_DAYS = 0
OUTPUT = 'public'  # The folder that becomes the website
os.makedirs(OUTPUT, exist_ok=True)
DEBUG_BANDS = os.environ.get('DEBUG_BANDS') == '1'
TARGET_CRS = 'EPSG:4326'
NH_SOURCE_REGION = [-179.5, 20.0, 179.5, 89.0]
NH_W_BOUNDS = [-179.5, 20.0, 0.0, 89.0]
NH_E_BOUNDS = [0.0, 20.0, 179.5, 89.0]
try:
    NH_LON0 = float(os.environ.get('NH_LON0', '80.0'))
except ValueError:
    NH_LON0 = 80.0

# Regions
NH_W = ee.Geometry.Rectangle([-179.5, 20.0, 0.0, 89.5], geodesic=False)
NH_E = ee.Geometry.Rectangle([0.0, 20.0, 179.5, 89.5], geodesic=False)
NH_REGION = NH_W.union(NH_E, maxError=1)
NA_REGION = ee.Geometry.Rectangle([-170.0, 10.0, -45.0, 80.0], geodesic=False)
CONUS_REGION = ee.Geometry.Rectangle([-127.0, 22.0, -65.0, 50.0], geodesic=False)
WORLD_REGION = ee.Geometry.Rectangle([-180.0, -89.9, 180.0, 89.9], geodesic=False)
NH_THUMB_REGION = [-180.0, 8.0, 20.0, 88.0]
NA_THUMB_REGION = [-170.0, 8.0, -40.0, 82.0]
CONUS_THUMB_REGION = [-127.0, 22.0, -65.0, 50.0]
# Northeast-only domain (excludes VA/NC by southern boundary; caps near tip of Maine)
NE_THUMB_REGION = [-82.5, 39.2, -66.0, 47.6]
# New England zoom snowfall domain: Delmarva, NJ, NYC/LI, southern New England/Boston corridor
NE_ZOOM_SNOW_THUMB_REGION = [-76.9, 38.0, -70.0, 43.3]

# Boundaries overlays
COUNTRIES = ee.FeatureCollection('USDOS/LSIB_SIMPLE/2017')
COUNTRIES_BORDERS = ee.FeatureCollection('USDOS/LSIB/2017')
US_STATES = ee.FeatureCollection('TIGER/2018/States')
NE_STATE_NAMES = [
    'Connecticut', 'Delaware', 'Maine', 'Maryland', 'Massachusetts',
    'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania',
    'Rhode Island', 'Vermont'
]
NE_STATES = US_STATES.filter(ee.Filter.inList('NAME', NE_STATE_NAMES))
NE_EXCLUDED_STATES = US_STATES.filter(ee.Filter.inList('NAME', ['Virginia', 'North Carolina']))

CONUS_SNOW_LABEL_AIRPORTS = [
    ('SEA', -122.3088, 47.4502),
    ('PDX', -122.5951, 45.5898),
    ('BOI', -116.2228, 43.5644),
    ('SFO', -122.3790, 37.6213),
    ('SMF', -121.5908, 38.6951),
    ('RNO', -119.7681, 39.4986),
    ('LAX', -118.4085, 33.9416),
    ('SAN', -117.1897, 32.7338),
    ('LAS', -115.1512, 36.0840),
    ('PHX', -112.0116, 33.4343),
    ('TUS', -110.9410, 32.1161),
    ('ABQ', -106.6090, 35.0402),
    ('SLC', -111.9778, 40.7884),
    ('DEN', -104.6737, 39.8561),
    ('ELP', -106.3778, 31.8073),
    ('DFW', -97.0403, 32.8998),
    ('AUS', -97.6699, 30.1975),
    ('SAT', -98.4698, 29.5337),
    ('IAH', -95.3368, 29.9902),
    ('HOU', -95.2789, 29.6454),
    ('OKC', -97.6007, 35.3931),
    ('ICT', -97.4331, 37.6499),
    ('OMA', -95.8941, 41.3032),
    ('MCI', -94.7139, 39.2976),
    ('STL', -90.3700, 38.7487),
    ('ORD', -87.9073, 41.9742),
    ('MKE', -87.8966, 42.9472),
    ('MSP', -93.2218, 44.8848),
    ('DTW', -83.3534, 42.2162),
    ('CLE', -81.8498, 41.4117),
    ('CMH', -82.8919, 39.9980),
    ('IND', -86.2944, 39.7173),
    ('PIT', -80.2329, 40.4915),
    ('CVG', -84.6678, 39.0488),
    ('ATL', -84.4277, 33.6407),
    ('BNA', -86.6689, 36.1263),
    ('MSY', -90.2580, 29.9934),
    ('RDU', -78.7875, 35.8776),
    ('IAD', -77.4565, 38.9531),
    ('BWI', -76.6684, 39.1754),
    ('PHL', -75.2424, 39.8729),
    ('BUF', -78.7322, 42.9405),
    ('CLT', -80.9431, 35.2140),
    ('JFK', -73.7781, 40.6413),
    ('BOS', -71.0052, 42.3656),
    ('PWM', -70.3093, 43.6462),
    ('TPA', -82.5332, 27.9755),
    ('MCO', -81.3089, 28.4312),
    ('JAX', -81.6879, 30.4941),
    ('MIA', -80.2906, 25.7959),
]
NE_SNOW_LABEL_AIRPORTS = [
    ('BUF', -78.7322, 42.9405),
    ('ROC', -77.6724, 43.1189),
    ('SYR', -76.1063, 43.1112),
    ('ABE', -75.4408, 40.6521),
    ('AVP', -75.7234, 41.3385),
    ('MDT', -76.7634, 40.1935),
    ('ALB', -73.8017, 42.7483),
    ('BTV', -73.1533, 44.4719),
    ('BDL', -72.6832, 41.9389),
    ('ORH', -71.8757, 42.2673),
    ('BOS', -71.0052, 42.3656),
    ('PVD', -71.4332, 41.7240),
    ('PWM', -70.3093, 43.6462),
    ('BGR', -68.8281, 44.8074),
    ('MHT', -71.4357, 42.9326),
    ('PSM', -70.8233, 43.0779),
    ('JFK', -73.7781, 40.6413),
    ('LGA', -73.8740, 40.7769),
    ('EWR', -74.1745, 40.6895),
    ('HPN', -73.7076, 41.0670),
    ('ISP', -73.1002, 40.7952),
    ('SWF', -74.1048, 41.5041),
    ('PHL', -75.2424, 39.8729),
    ('ACY', -74.5772, 39.4576),
]
NE_ZOOM_SNOW_LABEL_AIRPORTS = [
    ('DCA', -77.0377, 38.8521),
    ('BWI', -76.6684, 39.1754),
    ('ILG', -75.6065, 39.6787),
    ('PHL', -75.2424, 39.8729),
    ('ACY', -74.5772, 39.4576),
    ('EWR', -74.1745, 40.6895),
    ('LGA', -73.8740, 40.7769),
    ('JFK', -73.7781, 40.6413),
    ('ISP', -73.1002, 40.7952),
    ('SWF', -74.1048, 41.5041),
    ('BDL', -72.6832, 41.9389),
    ('PVD', -71.4332, 41.7240),
    ('ORH', -71.8757, 42.2673),
    ('BOS', -71.0052, 42.3656),
    ('MHT', -71.4357, 42.9326),
    ('PWM', -70.3093, 43.6462),
    ('SBY', -75.5103, 38.3405),
]


def _airport_features(airports):
    features = [ee.Feature(ee.Geometry.Point([lon, lat]), {'code': code}) for code, lon, lat in airports]
    lookup = {code: (lon, lat) for code, lon, lat in airports}
    return ee.FeatureCollection(features), lookup


CONUS_SNOW_AIRPORT_FC, CONUS_SNOW_AIRPORT_LOOKUP = _airport_features(CONUS_SNOW_LABEL_AIRPORTS)
NE_SNOW_AIRPORT_FC, NE_SNOW_AIRPORT_LOOKUP = _airport_features(NE_SNOW_LABEL_AIRPORTS)
NE_ZOOM_SNOW_AIRPORT_FC, NE_ZOOM_SNOW_AIRPORT_LOOKUP = _airport_features(NE_ZOOM_SNOW_LABEL_AIRPORTS)


def ts():
    return time.strftime('%Y-%m-%d %H:%M:%S')

# Forecast hour controls
hours_csv = os.environ.get('HOURS_CSV')
hours_step_env = os.environ.get('HOURS_STEP')
hours_max_env = os.environ.get('HOURS_MAX')
hours_limit_env = os.environ.get('HOURS_LIMIT')
run_init_utc_env = os.environ.get('RUN_INIT_UTC')
snow_ratio_csv_env = os.environ.get('SNOW_RATIO_CSV')
run_history_hours_env = os.environ.get('RUN_HISTORY_HOURS')
event_name = (os.environ.get('GITHUB_EVENT_NAME') or '').lower()
fast_render_env = os.environ.get('FAST_RENDER')
run_nh_z500a_env = os.environ.get('WN2_RUN_NH_Z500A')
run_na_z500a_env = os.environ.get('WN2_RUN_NA_Z500A')
run_conus_mslp_ptype_env = os.environ.get('WN2_RUN_CONUS_MSLP_PTYPE')
run_ne_mslp_ptype_env = os.environ.get('WN2_RUN_NE_MSLP_PTYPE')
run_conus_vort500_env = os.environ.get('WN2_RUN_CONUS_VORT500')
run_conus_snow_accum_env = os.environ.get('WN2_RUN_CONUS_SNOW_ACCUM')
run_ne_snow_accum_env = os.environ.get('WN2_RUN_NE_SNOW_ACCUM')
run_ne_zoom_snow_accum_env = os.environ.get('WN2_RUN_NE_ZOOM_SNOW_ACCUM')


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
    ANOMALY_DIMS = '1080x790'
    CONUS_DIMS = '1300x930'
    NE_DIMS = '1180x930'
    PTYPE_CONUS_DIMS = '1320x960'
    PTYPE_NE_DIMS = '1200x980'
    SNOW_CONUS_DIMS = '1320x960'
    SNOW_NE_DIMS = '1200x980'
    NH_SOURCE_DIMS = '2000x400'
    NH_POLAR_DIMS = 980
    ANOMALY_NA_SCALE_M = 36000
    ANOMALY_NH_SCALE_M = 50000
    ANOMALY_WORK_SCALE_M = 130000
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
    ANOMALY_NA_SCALE_M = 32000
    ANOMALY_NH_SCALE_M = 45000
    ANOMALY_WORK_SCALE_M = 120000

workers_env = os.environ.get('EXPORT_WORKERS')
try:
    EXPORT_WORKERS = int(workers_env) if workers_env else (2 if FAST_RENDER else 2)
except ValueError:
    EXPORT_WORKERS = 2
EXPORT_WORKERS = max(1, min(4, EXPORT_WORKERS))
print(f'[{ts()}] Render profile: fast={FAST_RENDER}, workers={EXPORT_WORKERS}, dims={ANOMALY_DIMS}/{CONUS_DIMS}.')

ANOMALY_PALETTE = [
    '#6f00a8', '#8f45c8', '#5f58cf', '#2f75e2', '#5fa8ef', '#9fd7f5',
    '#e8e8dd',
    '#f6e48e', '#f8c06b', '#f39a55', '#ea6e45', '#d93f2f', '#9a1f16'
]
ANOMALY_NEG_PALETTE = ['#6f00a8', '#8f45c8', '#5f58cf', '#2f75e2', '#5fa8ef', '#9fd7f5']
ANOMALY_POS_PALETTE = ['#f6e48e', '#f8c06b', '#f39a55', '#ea6e45', '#d93f2f', '#9a1f16']
ANOMALY_MIN_M = -300
ANOMALY_MAX_M = 300
ANOMALY_NEUTRAL_M = 8
ANOMALY_DISPLAY_GAIN = 1.05
ANOMALY_SMOOTH_RADIUS_PX = 1
BASEMAP_LAND_COLOR = '#dbe1e7'
BASEMAP_OCEAN_COLOR = '#c9d6e2'
VORTICITY_PALETTE = ['#f5ee00', '#f4c236', '#ee8c4a', '#d35a75', '#a03ca0', '#5f209f']
RAIN_RATE_PALETTE = ['#a9ee80', '#7ad35a', '#4eb744', '#2f9637', '#f7ea00', '#ffbf00', '#ff8a00', '#ff4200', '#b70000', '#c21cff']
SNOW_RATE_PALETTE = ['#0a1f6f', '#0d2f8f', '#1448b1', '#1f66cc', '#2d84df', '#45a6ef', '#63c2ff']
FRZR_RATE_PALETTE = ['#ffe5ef', '#ffc4da', '#f78fb9', '#f06292', '#d81b60', '#ad1457', '#880e4f']
SLEET_RATE_PALETTE = ['#f0d9ff', '#e1bee7', '#ce93d8', '#ab47bc', '#8e24aa', '#6a1b9a']
INCH_TO_MM = 25.4
PTYPE_RATE_MIN_MMHR = 0.02
PTYPE_RATE_MAX_MMHR = 12.0
SNOW_PTYPE_SEGMENTS_MMHR = [
    # 0.00-0.10 in/hr (medium -> darker blue)
    (0.0, 0.10 * INCH_TO_MM, ['#4ea6ff', '#2f84db', '#1f63bf']),
    # 0.10-0.25 in/hr (dark blue)
    (0.10 * INCH_TO_MM, 0.25 * INCH_TO_MM, ['#2b76d1', '#1e5fb8', '#0f3a88']),
    # 0.25-0.50 in/hr (dark purple)
    (0.25 * INCH_TO_MM, 0.50 * INCH_TO_MM, ['#4a148c', '#5e2aa8', '#7140bf']),
    # 0.50-1.00 in/hr (cyan)
    (0.50 * INCH_TO_MM, 1.00 * INCH_TO_MM, ['#b9ffff', '#58e4ef', '#00bfd3']),
]
SNOW_PTYPE_MAX_MMHR = 1.0 * INCH_TO_MM
SNOW_PTYPE_TICKS_MMHR = [0.0, 0.10 * INCH_TO_MM, 0.25 * INCH_TO_MM, 0.50 * INCH_TO_MM, 1.0 * INCH_TO_MM]
SNOW_ACCUM_STEP_SEGMENTS_IN = [
    (0.1, 2.0, ['#eaf8ff']),
    (2.0, 4.0, ['#cfeeff']),
    (4.0, 6.0, ['#a9defd']),
    (6.0, 8.0, ['#7bc9f7']),
    (8.0, 10.0, ['#4aaee8']),
    (10.0, 12.0, ['#2f8fd9']),
    (12.0, 14.0, ['#516dd0']),
    (14.0, 16.0, ['#6b52c6']),
    (16.0, 18.0, ['#8540be']),
    (18.0, 20.0, ['#9d36b7']),
    (20.0, 22.0, ['#b93db8']),
    (22.0, 24.0, ['#d451bb']),
    (24.0, 26.0, ['#c8f8ff']),
    (26.0, 28.0, ['#8defff']),
    (28.0, 30.0, ['#4fe4ff']),
    (30.0, 32.0, ['#00cee8']),
]
SNOW_ACCUM_MAX_IN = 32.0
SNOW_ACCUM_OVER_COLOR = '#d60000'
CLIMO_H500_COLLECTION = (
    ee.ImageCollection(CLIMO_ASSET)
    .select(CLIMO_H500_BAND)
    .filter(ee.Filter.calendarRange(CLIMO_START_YEAR, CLIMO_END_YEAR, 'year'))
)
CLIMO_H500_CACHE = {}

PRODUCT_OPTIONS = [
    ('nh_z500a', 'NH 500mb Height Anomaly', 'nh_z500a_*.jpg', run_nh_z500a_env),
    ('na_z500a', 'North America 500mb Height Anomaly', 'na_z500a_*.jpg', run_na_z500a_env),
    ('conus_mslp_ptype', 'CONUS MSLP + P-Type', 'conus_mslp_ptype_*.jpg', run_conus_mslp_ptype_env),
    ('ne_mslp_ptype', 'Northeast MSLP + P-Type', 'ne_mslp_ptype_*.jpg', run_ne_mslp_ptype_env),
    ('conus_vort500', 'CONUS 500mb Vorticity', 'conus_vort500_*.jpg', run_conus_vort500_env),
    ('conus_snow_accum', 'CONUS Snowfall Accumulation', 'conus_snow_accum_*.jpg', run_conus_snow_accum_env),
    ('ne_snow_accum', 'Northeast Snowfall Accumulation', 'ne_snow_accum_*.jpg', run_ne_snow_accum_env),
    ('ne_zoom_snow_accum', 'New England Zoom Snowfall Accumulation', 'ne_zoom_snow_accum_*.jpg', run_ne_zoom_snow_accum_env),
]
SNOW_PRODUCT_KEYS = {'conus_snow_accum', 'ne_snow_accum', 'ne_zoom_snow_accum'}

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
        'ne_zoom_snow_accum_*.jpg',
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


def parse_utc_timestamp(raw):
    if raw is None:
        return None
    text = str(raw).strip()
    # Workflow/manual inputs sometimes include wrapping quotes.
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ('"', "'"):
        text = text[1:-1].strip()
    if not text:
        return None

    # Accept both trailing Z and z from workflow inputs.
    iso_text = text[:-1] + '+00:00' if text.lower().endswith('z') else text
    if len(iso_text) >= 5 and iso_text[-5] in ('+', '-') and iso_text[-3] != ':':
        iso_text = iso_text[:-2] + ':' + iso_text[-2:]
    try:
        dt = datetime.fromisoformat(iso_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H",
        "%Y-%m-%d %H",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def format_utc_timestamp(dt):
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def get_selected_start_time_recent(ic, days=7, requested_start_time=None):
    if requested_start_time:
        requested_dt = parse_utc_timestamp(requested_start_time)
        if requested_dt is None:
            raise ValueError(
                f'RUN_INIT_UTC="{requested_start_time}" is invalid. '
                'Use an ISO UTC timestamp like 2026-02-22T12:00:00Z.'
            )
        requested_iso = format_utc_timestamp(requested_dt)
        print(f'[{ts()}] RUN_INIT_UTC requested: {requested_iso}')
        requested = ic.filter(ee.Filter.eq('start_time', requested_iso))
        requested_count = requested.size().getInfo()
        print(f'[{ts()}] Requested run frame count: {requested_count}')
        if requested_count == 0:
            raise ValueError(
                f'Requested RUN_INIT_UTC={requested_iso} was not found in {ASSET}. '
                'Confirm the init time exists in WeatherNext2.'
            )
        return requested_iso, ee.Date(requested_iso), requested, requested

    return get_latest_start_time_recent(ic, days=days)


# Select either explicit RUN_INIT_UTC or latest run from a recent time window.
collection = ee.ImageCollection(ASSET)
latest_start_time, latest_start_date, recent_collection, latest_start_collection = get_selected_start_time_recent(
    collection,
    days=7,
    requested_start_time=run_init_utc_env,
)

if latest_start_collection.size().getInfo() == 0:
    if run_init_utc_env:
        raise ValueError(f'Requested RUN_INIT_UTC={run_init_utc_env} returned no images.')
    raise ValueError('Latest WN2 run filter returned no images in the last 7 days.')


def _parse_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_snow_ratios(csv_value):
    if csv_value:
        parts = [p.strip() for p in str(csv_value).split(',') if p.strip()]
        parsed = sorted({v for v in (_parse_int(p) for p in parts) if v is not None and 10 <= v <= 20})
        if parsed:
            return parsed
    return [10, 12, 15, 20]


SNOW_RATIOS = _parse_snow_ratios(snow_ratio_csv_env)
run_history_hours_raw = _parse_int(run_history_hours_env)
if run_history_hours_raw is None:
    RUN_HISTORY_HOURS = 24
else:
    RUN_HISTORY_HOURS = max(6, min(72, run_history_hours_raw))

print(f'[{ts()}] Snow ratios selected: {SNOW_RATIOS}')
print(f'[{ts()}] Run history retention: {RUN_HISTORY_HOURS}h')
if run_init_utc_env:
    print(f'[{ts()}] Requested run init override: {run_init_utc_env}')
elif event_name == 'workflow_dispatch':
    print(f'[{ts()}] RUN_INIT_UTC not set for workflow_dispatch; selecting latest available run.')


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
print(f'Selected start_time: {latest_start_time}')


def parse_run_init_utc(ts_utc):
    parsed = parse_utc_timestamp(ts_utc)
    if parsed is not None:
        return parsed
    print(f'[{ts()}] Warning: could not parse run init "{ts_utc}", falling back to current UTC time.')
    return datetime.now(timezone.utc)


RUN_INIT_UTC = parse_run_init_utc(latest_start_time)
RUN_ID = RUN_INIT_UTC.strftime('%Y%m%d%H')
RUN_ID_LABEL = RUN_INIT_UTC.strftime('%Y-%m-%d %HZ')
RUNS_ROOT_DIR = Path(OUTPUT) / 'runs'
RUN_OUTPUT_DIR = RUNS_ROOT_DIR / RUN_ID
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f'[{ts()}] Run output directory: {RUN_OUTPUT_DIR}')


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
def contour_overlay(field, interval, color, opacity=0.82, smooth_px=1, thicken_px=0):
    # Draw contour lines by detecting quantization edges.
    smoothed = field.resample('bilinear')
    if smooth_px and smooth_px > 0:
        smoothed = smoothed.focalMean(int(smooth_px), 'circle', 'pixels')
    quantized = smoothed.divide(interval).round()
    edges = quantized.focalMax(1).neq(quantized.focalMin(1))
    if thicken_px and thicken_px > 0:
        edges = edges.focalMax(int(thicken_px))
    return edges.selfMask().visualize(palette=[color], opacity=opacity)


def highlight_iso_overlay(field, level, color='#2455ff', opacity=0.92, tolerance=1.2, smooth_px=1):
    smoothed = field.resample('bilinear')
    if smooth_px and smooth_px > 0:
        smoothed = smoothed.focalMean(int(smooth_px), 'circle', 'pixels')
    line = smoothed.subtract(float(level)).abs().lte(float(tolerance)).focalMax(1).selfMask()
    return line.visualize(palette=[color], opacity=opacity)


def border_overlay(include_states=False, state_names=None):
    country_lines = ee.Image().byte().paint(COUNTRIES_BORDERS, 1, 1).selfMask().visualize(palette=['#333333'])
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


def anomaly_overlay(anomaly_field):
    # Apply mild smoothing + gain for a cleaner, less noisy anomaly presentation.
    anomaly_vis = anomaly_field.resample('bilinear')
    if ANOMALY_SMOOTH_RADIUS_PX > 0:
        anomaly_vis = anomaly_vis.focalMean(ANOMALY_SMOOTH_RADIUS_PX, 'circle', 'pixels')
    if abs(ANOMALY_DISPLAY_GAIN - 1.0) > 1e-6:
        anomaly_vis = anomaly_vis.multiply(ANOMALY_DISPLAY_GAIN)
    anomaly_vis = anomaly_vis.clamp(ANOMALY_MIN_M, ANOMALY_MAX_M)
    return anomaly_vis.visualize(
        min=ANOMALY_MIN_M,
        max=ANOMALY_MAX_M,
        palette=ANOMALY_PALETTE,
    )


def pseudo_z500_anomaly_m(height_dam, radius_px=20):
    broad = height_dam.resample('bilinear').focalMean(radius=radius_px, kernelType='circle', units='pixels')
    return height_dam.subtract(broad).multiply(10).rename('z500_pseudo_anomaly_m')


def _wrap_day_of_year_filter(start_doy, end_doy):
    if start_doy >= 1 and end_doy <= 366:
        return ee.Filter.calendarRange(start_doy, end_doy, 'day_of_year')
    if start_doy < 1:
        return ee.Filter.Or(
            ee.Filter.calendarRange(start_doy + 366, 366, 'day_of_year'),
            ee.Filter.calendarRange(1, end_doy, 'day_of_year'),
        )
    return ee.Filter.Or(
        ee.Filter.calendarRange(start_doy, 366, 'day_of_year'),
        ee.Filter.calendarRange(1, end_doy - 366, 'day_of_year'),
    )


def z500_climo_1991_2020_m(valid_utc, region_geom=None, cache_tag='global'):
    doy = valid_utc.timetuple().tm_yday
    hour = int(valid_utc.hour)
    cache_key = (doy, hour, cache_tag)
    cached = CLIMO_H500_CACHE.get(cache_key)
    if cached is not None:
        return cached

    start_doy = doy - CLIMO_DOY_WINDOW_DAYS
    end_doy = doy + CLIMO_DOY_WINDOW_DAYS
    hour_collection = CLIMO_H500_COLLECTION.filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    if region_geom is not None:
        hour_collection = hour_collection.map(lambda im: ee.Image(im).clip(region_geom))
    window_collection = hour_collection.filter(_wrap_day_of_year_filter(start_doy, end_doy))
    fallback_collection = CLIMO_H500_COLLECTION.filter(
        ee.Filter.calendarRange(int(valid_utc.month), int(valid_utc.month), 'month')
    ).filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    if region_geom is not None:
        fallback_collection = fallback_collection.map(lambda im: ee.Image(im).clip(region_geom))
    climo = ee.Image(
        ee.Algorithms.If(
            window_collection.size().gt(0),
            window_collection.mean(),
            fallback_collection.mean(),
        )
    ).rename('z500_climo_m').resample('bilinear')
    CLIMO_H500_CACHE[cache_key] = climo
    return climo


def z500_anomaly_m(img, hour, region_geom=None, cache_tag='global'):
    valid_utc = RUN_INIT_UTC + timedelta(hours=int(hour))
    forecast_height_m = img.select(WN2_Z500_BAND).divide(9.80665)
    climo_height_m = z500_climo_1991_2020_m(valid_utc, region_geom=region_geom, cache_tag=cache_tag)
    if region_geom is not None:
        forecast_height_m = forecast_height_m.clip(region_geom)
        climo_height_m = climo_height_m.clip(region_geom)
    if cache_tag != 'nh_z500a':
        forecast_height_m = forecast_height_m.reproject(crs=TARGET_CRS, scale=ANOMALY_WORK_SCALE_M)
        climo_height_m = climo_height_m.reproject(crs=TARGET_CRS, scale=ANOMALY_WORK_SCALE_M)
    return forecast_height_m.subtract(climo_height_m).rename('z500_anomaly_m')


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


def inset_region_bbox(region, lon_pad=0.6, lat_pad=0.6):
    if not isinstance(region, list) or len(region) != 4:
        return region
    west, south, east, north = [float(v) for v in region]
    if east - west <= 2 * lon_pad or north - south <= 2 * lat_pad:
        return region
    return [
        max(-179.9, west + lon_pad),
        max(-89.9, south + lat_pad),
        min(179.9, east - lon_pad),
        min(89.9, north - lat_pad),
    ]


def region_dimensions(base_dims, region):
    if not isinstance(region, list) or len(region) != 4:
        return base_dims
    width = None
    if isinstance(base_dims, int):
        width = base_dims
    elif isinstance(base_dims, str) and 'x' in base_dims:
        try:
            width = int(base_dims.lower().split('x', 1)[0])
        except ValueError:
            width = None
    if width is None or width < 200:
        return base_dims

    west, south, east, north = [float(v) for v in region]
    lon_span = (east - west) if east >= west else (east + 360.0 - west)
    lat_span = max(0.2, north - south)
    mid_lat = max(-80.0, min(80.0, (south + north) * 0.5))
    x_span = max(0.2, lon_span * max(0.2, math.cos(math.radians(mid_lat))))
    aspect = max(0.7, min(3.8, x_span / lat_span))
    height = int(round(width / aspect))
    height = max(360, min(2200, height))
    return f'{width}x{height}'


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


def _draw_legend(draw, product_key, width, y, snow_ratio=10):
    label_font = load_font(22 if width >= 1200 else 18, bold=False)
    tick_font = load_font(16 if width >= 1200 else 14, bold=False)
    x_pad = 58 if width >= 1200 else 36
    bar_x = x_pad
    bar_w = width - 2 * x_pad
    bar_y = y + 26
    bar_h = 22

    if product_key in ('nh_z500a', 'na_z500a'):
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '500-hPa Height Anomaly (m, 1991-2020 normal)', fill=(22, 22, 22), font=label_font)
        _draw_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, ANOMALY_PALETTE)
        _draw_ticks(
            draw,
            bar_x,
            bar_y,
            bar_w,
            bar_h,
            [-300, -200, -100, -50, 0, 50, 100, 200, 300],
            ANOMALY_MIN_M,
            ANOMALY_MAX_M,
            tick_font,
        )
        return

    if product_key == 'conus_vort500':
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '500-hPa Relative Vorticity (x10^-5 s^-1)', fill=(22, 22, 22), font=label_font)
        _draw_tapered_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, VORTICITY_PALETTE)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48], 4, 50, tick_font)
        return

    if product_key in ('conus_snow_accum', 'ne_snow_accum', 'ne_zoom_snow_accum'):
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), f'Accumulated Snowfall Total (in, {int(snow_ratio)}:1 ratio)', fill=(22, 22, 22), font=label_font)
        snow_segments = [(low, high, palette) for low, high, palette in SNOW_ACCUM_STEP_SEGMENTS_IN]
        _draw_segmented_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, snow_segments, 0.1, SNOW_ACCUM_MAX_IN)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32], 0.1, SNOW_ACCUM_MAX_IN, tick_font)
        return

    if product_key in ('conus_mslp_ptype', 'ne_mslp_ptype'):
        draw.text((bar_x, y + 2), 'Precip Rate by Type (mm/hr; Snow bins from in/hr)', fill=(22, 22, 22), font=label_font)
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
            if name == 'Snow':
                snow_segments = [(lo, hi, pal) for lo, hi, pal in SNOW_PTYPE_SEGMENTS_MMHR]
                _draw_segmented_gradient_bar(draw, sx, gradient_y, slot_w, bar_h, snow_segments, 0.0, SNOW_PTYPE_MAX_MMHR)
                _draw_ticks(
                    draw,
                    sx,
                    gradient_y,
                    slot_w,
                    bar_h,
                    SNOW_PTYPE_TICKS_MMHR,
                    0.0,
                    SNOW_PTYPE_MAX_MMHR,
                    tick_font,
                )
            else:
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


def annotate_map_file(out_file, product_key, hour, map_region=None, low_center=None, snow_labels=None, snow_ratio=10):
    from PIL import Image, ImageDraw

    product_titles = {
        'nh_z500a': 'WN2 0.25 deg | 500-hPa Geopotential Height (dam) & Anomaly vs 1991-2020 (m) | Northern Hemisphere',
        'na_z500a': 'WN2 0.25 deg | 500-hPa Geopotential Height (dam) & Anomaly vs 1991-2020 (m) | North America',
        'conus_mslp_ptype': 'WN2 0.25 deg | MSLP (hPa) + Precip Type | CONUS',
        'ne_mslp_ptype': 'WN2 0.25 deg | MSLP (hPa) + Precip Type | Northeast',
        'conus_vort500': 'WN2 0.25 deg | 500-hPa Relative Vorticity + 500-hPa Height (dam) | CONUS',
        'conus_snow_accum': f'WN2 0.25 deg | Accumulated Snowfall (in, {int(snow_ratio)}:1) | CONUS',
        'ne_snow_accum': f'WN2 0.25 deg | Accumulated Snowfall (in, {int(snow_ratio)}:1) | Northeast',
        'ne_zoom_snow_accum': f'WN2 0.25 deg | Accumulated Snowfall (in, {int(snow_ratio)}:1) | New England Zoom',
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

        if map_region is not None and snow_labels:
            snow_draw = ImageDraw.Draw(img)
            snow_font = load_font(20 if img.width >= 1300 else 16, bold=True)
            placed = []
            for item in sorted(snow_labels, key=lambda s: s.get('inches', 0.0), reverse=True):
                code = item.get('code')
                inches = item.get('inches')
                if not code or inches is None or float(inches) < 0.5:
                    continue
                marker_xy = _lonlat_to_image_xy(
                    item.get('lon'),
                    item.get('lat'),
                    map_region,
                    img.width,
                    img.height,
                )
                if marker_xy is None:
                    continue
                x, y = marker_xy
                text = f'{code} {int(round(float(inches)))}'
                tw, th = _text_size(snow_draw, text, snow_font)
                tx = max(4, min(img.width - tw - 4, x + 4))
                ty = max(4, min(img.height - th - 4, y - th - 2))
                rect = (tx - 2, ty - 1, tx + tw + 2, ty + th + 1)
                overlaps = False
                for ox0, oy0, ox1, oy1 in placed:
                    if not (rect[2] < ox0 or rect[0] > ox1 or rect[3] < oy0 or rect[1] > oy1):
                        overlaps = True
                        break
                if overlaps:
                    continue
                try:
                    snow_draw.text((tx, ty), text, fill=(32, 32, 32), font=snow_font, stroke_width=2, stroke_fill=(245, 245, 245))
                except TypeError:
                    snow_draw.text((tx, ty), text, fill=(32, 32, 32), font=snow_font)
                placed.append(rect)

        legend_h = 0
        if product_key in ('conus_mslp_ptype', 'ne_mslp_ptype'):
            legend_h = 180
        elif product_key in ('nh_z500a', 'na_z500a', 'conus_vort500', 'conus_snow_accum', 'ne_snow_accum', 'ne_zoom_snow_accum'):
            legend_h = 96

        header_h = 78
        footer_h = 30
        canvas = Image.new('RGB', (img.width, img.height + header_h + legend_h + footer_h), color=(236, 236, 236))
        canvas.paste(img, (0, header_h))
        draw = ImageDraw.Draw(canvas)

        title_font = load_font(30 if img.width >= 1300 else 26, bold=True)
        subtitle_font = load_font(22 if img.width >= 1300 else 19, bold=False)
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
            _draw_legend(draw, product_key, img.width, header_h + img.height + 2, snow_ratio=snow_ratio)

        footer_text = 'Source: WeatherNext2 (Earth Engine) | Generated by: Jonathan Wall (@_jwall on X)'
        footer_font = _fit_font(
            draw,
            footer_text,
            start_size=(18 if img.width >= 1300 else 16),
            min_size=12,
            bold=True,
            max_width=max_text_w,
        )
        draw.text((12, img.height + header_h + legend_h + 4), footer_text, fill=(28, 28, 28), font=footer_font)
        canvas.save(out_file, format='JPEG', quality=97, subsampling=0)


def remap_nh_to_polar(out_file, lon0=NH_LON0, lat_min=20.0, lat_max=88.5):
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
    tan_edge = math.tan((math.pi / 4.0) - (math.radians(lat_min) / 2.0))
    if tan_edge <= 0:
        tan_edge = 1e-6

    for y in range(out_size):
        dy = (y - cy) / radius
        for x in range(out_size):
            dx = (x - cx) / radius
            r = math.hypot(dx, dy)
            if r > 1.0:
                continue

            t = r * tan_edge
            lat = math.degrees((math.pi / 2.0) - (2.0 * math.atan(t)))
            lat = max(lat_min, min(lat_max, lat))
            if r < 1e-9:
                lon = lon0
            else:
                lon = lon0 - math.degrees(math.atan2(dx, -dy))
            lon = ((lon + 180.0) % 360.0) - 180.0

            # Wrap longitudes across the antimeridian to avoid a seam in polar remap.
            sx_f = ((lon + 180.0) / 360.0) * sw
            sx_f = sx_f % sw
            sy_f = (lat_max - lat) / (lat_max - lat_min) * (sh - 1)
            if sy_f < 0.0:
                sy_f = 0.0
            elif sy_f > (sh - 1):
                sy_f = float(sh - 1)

            x0_raw = int(math.floor(sx_f))
            x0 = x0_raw % sw
            y0 = int(math.floor(sy_f))
            x1 = (x0 + 1) % sw
            y1 = min(sh - 1, y0 + 1)
            wx = sx_f - x0_raw
            wy = sy_f - y0

            c00 = src_px[x0, y0]
            c10 = src_px[x1, y0]
            c01 = src_px[x0, y1]
            c11 = src_px[x1, y1]

            w00 = (1.0 - wx) * (1.0 - wy)
            w10 = wx * (1.0 - wy)
            w01 = (1.0 - wx) * wy
            w11 = wx * wy

            rch = int(round(c00[0] * w00 + c10[0] * w10 + c01[0] * w01 + c11[0] * w11))
            gch = int(round(c00[1] * w00 + c10[1] * w10 + c01[1] * w01 + c11[1] * w11))
            bch = int(round(c00[2] * w00 + c10[2] * w10 + c01[2] * w01 + c11[2] * w11))
            out_px[x, y] = (rch, gch, bch)

    draw = ImageDraw.Draw(out_img)
    draw.ellipse((cx - radius, cy - radius, cx + radius, cy + radius), outline=(44, 44, 44), width=2)

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
    current_region = region
    current_dimensions = dimensions
    current_scale = scale

    for attempt in range(1, 5):
        params = {
            'region': current_region,
            'format': 'jpg',
        }
        if crs is not None:
            params['crs'] = crs
        elif current_scale is not None:
            params['crs'] = TARGET_CRS
        if current_scale is not None:
            params['scale'] = current_scale
        else:
            params['dimensions'] = current_dimensions

        try:
            download_thumb(composite, out_file, params)
            print(f'[{ts()}] Export complete for {out_file} ({time.time() - t0:.2f}s)')
            return
        except requests.HTTPError as e:
            response = getattr(e, 'response', None)
            status = response.status_code if response is not None else None
            body = response.text if response is not None else str(e)
            is_memory = status == 400 and 'User memory limit exceeded' in body
            is_transform = status == 400 and 'Unable to transform edge' in body

            if attempt < 4 and is_memory:
                if current_scale is not None:
                    current_scale = int(max(2000, current_scale * 1.45))
                else:
                    current_dimensions = shrink_dimensions(current_dimensions)
                print(f'[{ts()}] Retry {attempt}/3 for {out_file} after memory limit.')
                continue

            if attempt < 4 and is_transform:
                new_region = inset_region_bbox(current_region)
                if new_region != current_region:
                    current_region = new_region
                    print(f'[{ts()}] Retry {attempt}/3 for {out_file} with inset region to bypass transform edge.')
                    continue

            raise

    print(f'[{ts()}] Export complete for {out_file} ({time.time() - t0:.2f}s)')


def generate_z500_anomaly_map(img, h, region, prefix):
    if prefix == 'nh_z500a':
        region_geom = ee.Geometry.Rectangle(NH_SOURCE_REGION, geodesic=False)
        export_region = NH_SOURCE_REGION
        export_crs = TARGET_CRS
        map_dims = NH_SOURCE_DIMS
    else:
        region_geom = ee.Geometry.Rectangle(region, geodesic=False)
        export_region = region
        export_crs = None
        map_dims = region_dimensions(ANOMALY_DIMS, region)

    forecast_height_dam = img.select(WN2_Z500_BAND).divide(9.80665).divide(10).clip(region_geom)
    contour_field = forecast_height_dam.reproject(crs=TARGET_CRS, scale=ANOMALY_WORK_SCALE_M)

    def _build_composite(anomaly_field, contour_interval=6):
        anomaly_layer = anomaly_overlay(anomaly_field)
        z500_contours = contour_overlay(
            contour_field,
            interval=contour_interval,
            color='#151515',
            opacity=0.88,
            smooth_px=0,
            thicken_px=1,
        )
        z540_contour = highlight_iso_overlay(
            contour_field,
            level=540,
            color='#2455ff',
            opacity=0.92,
            tolerance=1.0,
            smooth_px=0,
        )
        overlays = [
            basemap_overlay(region_geom, land_color=BASEMAP_LAND_COLOR, ocean_color=BASEMAP_OCEAN_COLOR),
            anomaly_layer,
            z500_contours,
            z540_contour,
            border_overlay(include_states=False).clip(region_geom),
        ]
        return ee.ImageCollection(overlays).mosaic()

    def _export_and_annotate(composite_image, use_scale=True):
        out_file = build_frame_path(prefix, h)
        if prefix == 'nh_z500a':
            if use_scale:
                export_composite(
                    composite_image,
                    out_file,
                    export_region,
                    scale=ANOMALY_NH_SCALE_M,
                    crs=export_crs,
                )
            else:
                export_composite(
                    composite_image,
                    out_file,
                    export_region,
                    dimensions=map_dims,
                    crs=export_crs,
                )
            remap_nh_to_polar(out_file, lon0=NH_LON0)
            annotate_map_file(out_file, prefix, h)
            return

        export_composite(
            composite_image,
            out_file,
            export_region,
            dimensions=map_dims,
            scale=(ANOMALY_NA_SCALE_M if use_scale else None),
            crs=export_crs,
        )
        annotate_map_file(out_file, prefix, h)

    def _is_recoverable_export_error(msg):
        return (
            'User memory limit exceeded' in msg
            or 'Unable to transform edge' in msg
            or 'Invalid argument' in msg
        )

    true_anomaly = z500_anomaly_m(img, h, region_geom=region_geom, cache_tag=prefix)
    if prefix == 'nh_z500a':
        plans = [
            {'use_scale': True, 'contour_interval': 6, 'label': 'scale + 6dm contours'},
            {'use_scale': True, 'contour_interval': 12, 'label': 'scale + 12dm contours'},
            {'use_scale': False, 'contour_interval': 12, 'label': 'dims + 12dm contours'},
        ]
    else:
        plans = [
            {'use_scale': True, 'contour_interval': 6, 'label': 'scale + 6dm contours'},
            {'use_scale': False, 'contour_interval': 6, 'label': 'dims + 6dm contours'},
            {'use_scale': False, 'contour_interval': 12, 'label': 'dims + 12dm contours'},
        ]

    last_msg = ''
    for plan in plans:
        try:
            _export_and_annotate(
                _build_composite(true_anomaly, contour_interval=plan['contour_interval']),
                use_scale=plan['use_scale'],
            )
            return
        except Exception as e:
            msg = str(e)
            if not _is_recoverable_export_error(msg):
                raise
            last_msg = msg
            print(f'[{ts()}] {prefix} hour {h}: true anomaly export retry failed ({plan["label"]}).')

    short_msg = (last_msg or 'Unknown export error').replace('\n', ' ')[:220]
    raise RuntimeError(
        f'{prefix} hour {h}: true-anomaly-only export failed after retries (fallback disabled). Last error: {short_msg}'
    )


def derive_precip_phase(img, region_geom):
    precip_6h_mm = img.select(WN2_PRECIP_6H_BAND).multiply(1000).clip(region_geom)
    precip_rate = precip_6h_mm.divide(6)  # mm/hr
    precip_rate_sm = precip_rate.focalMean(1, 'circle', 'pixels')
    precip_mask = precip_rate_sm.gt(0.12)

    t2c = img.select(WN2_T2M_BAND).subtract(273.15).clip(region_geom)
    t850c = img.select(WN2_T850_BAND).subtract(273.15).clip(region_geom)
    t700c = img.select(WN2_T700_BAND).subtract(273.15).clip(region_geom)

    snow = precip_mask.And(t2c.lte(1)).And(t850c.lte(-1)).And(t700c.lte(-2))
    freezing_rain = precip_mask.And(t2c.lte(0)).And(t850c.gt(1)).And(t700c.gt(-2))
    sleet = precip_mask.And(t2c.lte(0)).And(t850c.gt(0)).And(t700c.lte(-2)).And(freezing_rain.Not())
    rain = precip_mask.And(snow.Not()).And(freezing_rain.Not()).And(sleet.Not())
    return precip_rate_sm, precip_6h_mm, rain, snow, freezing_rain, sleet


def smooth_precip_type_masks(precip_rate, rain, snow, freezing_rain, sleet):
    precip_mask = precip_rate.gt(0.2).focalMax(1, 'circle', 'pixels').focalMin(1, 'circle', 'pixels')
    ptype = ee.Image.constant(0).updateMask(precip_mask)
    ptype = ptype.where(snow, 1)
    ptype = ptype.where(freezing_rain, 2)
    ptype = ptype.where(sleet, 3)
    ptype_sm = ptype.focalMode(2, 'circle', 'pixels').focalMode(1, 'circle', 'pixels').updateMask(precip_mask)
    rain_sm = ptype_sm.eq(0).And(precip_mask)
    snow_sm = ptype_sm.eq(1).And(precip_mask)
    frz_sm = ptype_sm.eq(2).And(precip_mask)
    sleet_sm = ptype_sm.eq(3).And(precip_mask)
    return rain_sm, snow_sm, frz_sm, sleet_sm


def snow_ptype_rate_layer(precip_rate_mmhr, snow_mask):
    snow_rate = precip_rate_mmhr.updateMask(snow_mask)
    layers = [
        range_gradient_layer(snow_rate, low, high, palette, include_high=(i == len(SNOW_PTYPE_SEGMENTS_MMHR) - 1))
        for i, (low, high, palette) in enumerate(SNOW_PTYPE_SEGMENTS_MMHR)
    ]
    layers.append(snow_rate.gt(SNOW_PTYPE_MAX_MMHR).selfMask().visualize(palette=['#00a8c1']))
    return ee.ImageCollection(layers).mosaic()


def find_low_center(mslp_hpa, region_geom, fallback_region=None, scale_m=50000):
    def _fallback(mb_value=None):
        if fallback_region and len(fallback_region) == 4:
            west, south, east, north = fallback_region
            return {
                'lon': (west + east) / 2.0,
                'lat': (south + north) / 2.0,
                'mb': mb_value,
            }
        return None

    min_val = None
    try:
        min_stats = mslp_hpa.reduceRegion(
            reducer=ee.Reducer.min(),
            geometry=region_geom,
            scale=scale_m,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=4,
        ).getInfo() or {}
        min_raw = min_stats.get(WN2_MSLP_BAND)
        if min_raw is None:
            return _fallback()
        min_val = float(min_raw)
        mb_value = int(round(min_val))
    except Exception as e:
        print(f'[{ts()}] Low-center min reduction failed: {e}')
        return _fallback()

    try:
        min_mask = mslp_hpa.lte(min_val + 0.2).selfMask()
        lonlat_img = ee.Image.pixelLonLat().updateMask(min_mask)

        sample = lonlat_img.sample(
            region=region_geom,
            scale=scale_m,
            numPixels=1,
            dropNulls=True,
            geometries=False,
            tileScale=4,
            seed=42,
        ).first()
        if sample is not None:
            info = sample.getInfo() or {}
            props = info.get('properties') or {}
            lon = props.get('longitude')
            lat = props.get('latitude')
            if lon is not None and lat is not None:
                return {'lon': float(lon), 'lat': float(lat), 'mb': mb_value}

        lonlat = lonlat_img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region_geom,
            scale=scale_m,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=4,
        ).getInfo() or {}
        lon = lonlat.get('longitude')
        lat = lonlat.get('latitude')
        if lon is not None and lat is not None:
            return {'lon': float(lon), 'lat': float(lat), 'mb': mb_value}
    except Exception as e:
        print(f'[{ts()}] Low-center coordinate detection failed: {e}')

    return _fallback(mb_value)


def get_snow_airport_labels(snow_total_in, key):
    if key == 'ne_zoom_snow_accum':
        airport_fc = NE_ZOOM_SNOW_AIRPORT_FC
        lookup = NE_ZOOM_SNOW_AIRPORT_LOOKUP
    elif key.startswith('ne_'):
        airport_fc = NE_SNOW_AIRPORT_FC
        lookup = NE_SNOW_AIRPORT_LOOKUP
    else:
        airport_fc = CONUS_SNOW_AIRPORT_FC
        lookup = CONUS_SNOW_AIRPORT_LOOKUP
    try:
        samples = snow_total_in.rename('snow_in').sampleRegions(
            collection=airport_fc,
            properties=['code'],
            scale=25000,
            geometries=False,
            tileScale=4,
        ).getInfo() or {}
        labels = []
        for feat in samples.get('features', []):
            props = feat.get('properties') or {}
            code = props.get('code')
            val = props.get('snow_in')
            if code is None or val is None:
                continue
            if code not in lookup:
                continue
            lon, lat = lookup[code]
            labels.append({
                'code': code,
                'inches': max(0.0, float(val)),
                'lon': lon,
                'lat': lat,
            })
        return labels
    except Exception as e:
        print(f'[{ts()}] Snow airport labels skipped: {e}')
        return []


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
    layers = []
    for idx, (low, high, palette) in enumerate(SNOW_ACCUM_STEP_SEGMENTS_IN):
        layers.append(
            range_gradient_layer(
                snow_total_in,
                low,
                high,
                palette,
                include_high=(idx == len(SNOW_ACCUM_STEP_SEGMENTS_IN) - 1),
            )
        )
    layers.append(snow_total_in.gt(SNOW_ACCUM_MAX_IN).selfMask().visualize(palette=[SNOW_ACCUM_OVER_COLOR]))
    return ee.ImageCollection(layers).mosaic()


def is_snow_product(product_key):
    return product_key in SNOW_PRODUCT_KEYS


def build_frame_name(product_key, hour, snow_ratio=None):
    if is_snow_product(product_key):
        ratio = int(round(float(snow_ratio if snow_ratio is not None else 10)))
        return f'{product_key}_r{ratio:02d}_{hour:03d}.jpg'
    return f'{product_key}_{hour:03d}.jpg'


def build_frame_path(product_key, hour, snow_ratio=None):
    return str(RUN_OUTPUT_DIR / build_frame_name(product_key, hour, snow_ratio=snow_ratio))


def generate_mslp_ptype_map(img, h, region=CONUS_THUMB_REGION, key='conus_mslp_ptype'):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    is_ne = key.startswith('ne_')
    state_names = NE_STATE_NAMES if is_ne else None
    land_fc = NE_STATES if is_ne else None
    work_geom = region_geom.difference(NE_EXCLUDED_STATES.geometry(), maxError=1000) if is_ne else region_geom
    precip_rate, _, rain, snow, freezing_rain, sleet = derive_precip_phase(img, work_geom)
    rain_sm, snow_sm, frz_sm, sleet_sm = smooth_precip_type_masks(precip_rate, rain, snow, freezing_rain, sleet)
    precip_rate_vis = precip_rate.resample('bilinear').focalMean(2, 'circle', 'pixels')

    rain_layer = precip_rate_vis.updateMask(rain_sm).visualize(
        min=PTYPE_RATE_MIN_MMHR, max=PTYPE_RATE_MAX_MMHR,
        palette=RAIN_RATE_PALETTE,
    )
    snow_layer = snow_ptype_rate_layer(precip_rate_vis, snow_sm)
    frz_layer = precip_rate_vis.updateMask(frz_sm).visualize(
        min=PTYPE_RATE_MIN_MMHR, max=PTYPE_RATE_MAX_MMHR,
        palette=FRZR_RATE_PALETTE,
    )
    sleet_layer = precip_rate_vis.updateMask(sleet_sm).visualize(
        min=PTYPE_RATE_MIN_MMHR, max=PTYPE_RATE_MAX_MMHR,
        palette=SLEET_RATE_PALETTE,
    )

    mslp_hpa = img.select(WN2_MSLP_BAND).divide(100).clip(work_geom)
    mslp_contours = contour_overlay(
        mslp_hpa,
        interval=3,
        color='#2a2a2a',
        opacity=0.9,
    )
    low_center = find_low_center(mslp_hpa, work_geom, fallback_region=region)

    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color=BASEMAP_LAND_COLOR, ocean_color=BASEMAP_OCEAN_COLOR, land_fc=land_fc),
        rain_layer,
        snow_layer,
        frz_layer,
        sleet_layer,
        mslp_contours,
        border_overlay(include_states=True, state_names=state_names),
    ]).mosaic()

    out_file = build_frame_path(key, h)
    base_dims = PTYPE_NE_DIMS if key.startswith('ne_') else PTYPE_CONUS_DIMS
    dims = region_dimensions(base_dims, region)
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h, map_region=region, low_center=low_center)


def generate_snow_accum_map(img, h, running_snow_cm, region=CONUS_THUMB_REGION, key='conus_snow_accum', snow_ratio=10):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    is_ne = key.startswith('ne_')
    state_names = NE_STATE_NAMES if is_ne else None
    land_fc = NE_STATES if is_ne else None
    work_geom = region_geom.difference(NE_EXCLUDED_STATES.geometry(), maxError=1000) if is_ne else region_geom
    ratio_scale = ee.Image.constant(float(snow_ratio) / 10.0)
    snow_total_in = running_snow_cm.multiply(ratio_scale).divide(2.54).clip(work_geom)
    snow_total_vis = snow_total_in.resample('bilinear').focalMean(1, 'circle', 'pixels')
    snow_layer = snow_accum_layer(snow_total_vis)
    snow_labels = get_snow_airport_labels(snow_total_in, key)

    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color=BASEMAP_LAND_COLOR, ocean_color=BASEMAP_OCEAN_COLOR, land_fc=land_fc),
        snow_layer,
        border_overlay(include_states=True, state_names=state_names),
    ]).mosaic()

    out_file = build_frame_path(key, h, snow_ratio=snow_ratio)
    base_dims = SNOW_NE_DIMS if key.startswith('ne_') else SNOW_CONUS_DIMS
    dims = region_dimensions(base_dims, region)
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h, map_region=region, snow_labels=snow_labels, snow_ratio=snow_ratio)


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
        basemap_overlay(region_geom, land_color=BASEMAP_LAND_COLOR, ocean_color=BASEMAP_OCEAN_COLOR),
        vort_layer,
        z500_contours,
        border_overlay(include_states=True),
    ]).mosaic()

    out_file = build_frame_path('conus_vort500', h)
    dims = region_dimensions(CONUS_DIMS, CONUS_THUMB_REGION)
    export_composite(composite, out_file, CONUS_THUMB_REGION, dimensions=dims)
    annotate_map_file(out_file, 'conus_vort500', h)


def _file_md5(p: Path) -> str:
    h = hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sanity_check_jpgs(out_dir: str, pattern: str = "z500a_*.jpg", require_variation=True) -> None:
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
    if require_variation and len(files) > 1 and len(unique_hashes) == 1:
        raise RuntimeError("Sanity check failed: all images are identical (same MD5).")

    print(f"Sanity OK: {len(files)} images, {len(unique_hashes)} unique hashes.")


# --- 4. EXECUTION ---
cleanup_old_products()
failures = []
successful_exports = 0
needs_snow_accum = any(k in SNOW_PRODUCT_KEYS for k, _, _ in ENABLED_PRODUCTS)
snow_accum_by_hour = {}

if needs_snow_accum:
    zero_snow = ee.Image.constant(0).clip(CONUS_REGION).rename('snow_total_cm')
    selected_hours = sorted(set(HOURS))
    max_selected_hour = selected_hours[-1]
    accum_hours = sorted([hh for hh in AVAILABLE_HOURS if 0 <= hh <= max_selected_hour])
    if not accum_hours:
        accum_hours = selected_hours

    running_snow_cm = zero_snow
    running_by_available_hour = {0: zero_snow}
    for hh in accum_hours:
        if hh <= 0:
            running_by_available_hour[hh] = running_snow_cm
            continue
        step_img = get_hour_image(hh)
        running_snow_cm = running_snow_cm.add(snow_increment_cm(step_img, CONUS_REGION)).rename('snow_total_cm')
        running_by_available_hour[hh] = running_snow_cm

    for sh in selected_hours:
        eligible = [hh for hh in running_by_available_hour.keys() if hh <= sh]
        if eligible:
            snow_accum_by_hour[sh] = running_by_available_hour[max(eligible)]
        else:
            snow_accum_by_hour[sh] = zero_snow
    print(
        f'[{ts()}] Snow accumulation precompute: '
        f'available_steps={len(accum_hours)}, selected_frames={len(selected_hours)}, '
        f'max_hour={max_selected_hour}.'
    )
else:
    zero_snow = ee.Image.constant(0).clip(CONUS_REGION).rename('snow_total_cm')

for h in HOURS:
    print(f'Generating Hour {h}...')
    img = get_hour_image(h)
    snow_for_hour = snow_accum_by_hour.get(h, zero_snow)

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
        for ratio in SNOW_RATIOS:
            tasks.append((
                f'conus_snow_accum_r{ratio:02d}',
                lambda i=img, hh=h, s=snow_for_hour, rr=ratio: generate_snow_accum_map(
                    i,
                    hh,
                    s,
                    CONUS_THUMB_REGION,
                    'conus_snow_accum',
                    snow_ratio=rr,
                ),
            ))
    if 'ne_snow_accum' in enabled_keys:
        for ratio in SNOW_RATIOS:
            tasks.append((
                f'ne_snow_accum_r{ratio:02d}',
                lambda i=img, hh=h, s=snow_for_hour, rr=ratio: generate_snow_accum_map(
                    i,
                    hh,
                    s,
                    NE_THUMB_REGION,
                    'ne_snow_accum',
                    snow_ratio=rr,
                ),
            ))
    if 'ne_zoom_snow_accum' in enabled_keys:
        for ratio in SNOW_RATIOS:
            tasks.append((
                f'ne_zoom_snow_accum_r{ratio:02d}',
                lambda i=img, hh=h, s=snow_for_hour, rr=ratio: generate_snow_accum_map(
                    i,
                    hh,
                    s,
                    NE_ZOOM_SNOW_THUMB_REGION,
                    'ne_zoom_snow_accum',
                    snow_ratio=rr,
                ),
            ))
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
    for key, _, pattern in ENABLED_PRODUCTS:
        sanity_check_jpgs(
            str(RUN_OUTPUT_DIR),
            pattern=pattern,
            require_variation=not is_snow_product(key),
        )
else:
    print(f'[{ts()}] Skipping sanity check: no product images were created.')


# --- 5. BUILD INTERFACE ---
def parse_iso_utc(raw):
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def normalize_int_list(values, min_value=None, max_value=None):
    items = []
    if isinstance(values, list):
        for raw in values:
            v = _parse_int(raw)
            if v is None:
                continue
            if min_value is not None and v < min_value:
                continue
            if max_value is not None and v > max_value:
                continue
            items.append(v)
    return sorted(set(items))


def load_manifest_entries(manifest_path):
    if not manifest_path.exists():
        return []
    try:
        with manifest_path.open('r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as e:
        print(f'[{ts()}] Could not read existing runs manifest: {e}')
        return []
    runs = payload.get('runs') if isinstance(payload, dict) else payload
    if not isinstance(runs, list):
        return []
    entries = []
    for item in runs:
        if isinstance(item, dict) and item.get('id'):
            entries.append(item)
    return entries


manifest_path = Path(OUTPUT) / 'runs_manifest.json'
existing_entries = load_manifest_entries(manifest_path)
now_utc = datetime.now(timezone.utc)
cutoff_utc = now_utc - timedelta(hours=RUN_HISTORY_HOURS)
enabled_keys = [key for key, _, _ in ENABLED_PRODUCTS]
default_ratios = SNOW_RATIOS if any(k in SNOW_PRODUCT_KEYS for k in enabled_keys) else [10]
current_entry = {
    'id': RUN_ID,
    'label': RUN_ID_LABEL,
    'run_date': run_date,
    'init_utc': iso_utc(RUN_INIT_UTC),
    'hours': HOURS,
    'products': enabled_keys,
    'snow_ratios': default_ratios,
    'updated_utc': iso_utc(now_utc),
}

merged = {}
for entry in [current_entry] + existing_entries:
    rid = str(entry.get('id', '')).strip()
    if not rid or rid in merged:
        continue
    init_dt = parse_iso_utc(entry.get('init_utc'))
    if init_dt is None:
        continue
    # Retain runs by publication/update recency so manual backfills remain visible
    # for the configured history window even when their model init is older.
    updated_dt = parse_iso_utc(entry.get('updated_utc'))
    retention_dt = updated_dt or init_dt
    if rid != RUN_ID and retention_dt < cutoff_utc:
        continue
    run_dir = RUNS_ROOT_DIR / rid
    if rid != RUN_ID and not run_dir.exists():
        continue
    products = [str(p) for p in entry.get('products', []) if isinstance(p, str)]
    if not products:
        continue
    snow_ratios = normalize_int_list(entry.get('snow_ratios', []), min_value=10, max_value=20)
    if not snow_ratios:
        snow_ratios = [10]
    hours = normalize_int_list(entry.get('hours', []), min_value=0)
    if not hours:
        continue
    merged[rid] = {
        'id': rid,
        'label': str(entry.get('label') or rid),
        'run_date': str(entry.get('run_date') or rid),
        'init_utc': iso_utc(init_dt),
        'hours': hours,
        'products': products,
        'snow_ratios': snow_ratios,
        'updated_utc': str(entry.get('updated_utc') or iso_utc(now_utc)),
    }

manifest_runs = sorted(
    merged.values(),
    key=lambda item: parse_iso_utc(item.get('init_utc')) or datetime.min.replace(tzinfo=timezone.utc),
    reverse=True,
)
valid_run_ids = {item['id'] for item in manifest_runs}
if RUNS_ROOT_DIR.exists():
    for child in RUNS_ROOT_DIR.iterdir():
        if child.is_dir() and child.name not in valid_run_ids:
            shutil.rmtree(child, ignore_errors=True)

manifest_payload = {
    'generated_utc': iso_utc(now_utc),
    'history_hours': RUN_HISTORY_HOURS,
    'default_run_id': RUN_ID,
    'snow_products': sorted(SNOW_PRODUCT_KEYS),
    'product_labels': {key: label for key, label, _, _ in PRODUCT_OPTIONS},
    'runs': manifest_runs,
}
with manifest_path.open('w', encoding='utf-8') as f:
    json.dump(manifest_payload, f, indent=2)

manifest_json = json.dumps(manifest_payload, separators=(',', ':'))
html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>WN2 Multi-Product Viewer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root {
            --controls-h: 188px;
        }
        body {
            background:#1f1f1f;
            color:#efefef;
            font-family:system-ui, sans-serif;
            text-align:center;
            margin:0;
            padding-bottom:0;
        }
        .wrap { max-width:1240px; margin:0 auto; padding:14px 10px 10px; }
        .map-wrap {
            background:#111;
            border:1px solid #4f4f4f;
            height:60vh;
            min-height:220px;
            display:flex;
            align-items:center;
            justify-content:center;
            overflow:hidden;
        }
        img {
            width:auto;
            height:auto;
            max-width:100%;
            max-height:100%;
            object-fit:contain;
            display:block;
            background:#111;
        }
        #title {
            margin:6px 0 10px;
            font-size:24px;
            font-weight:700;
            letter-spacing:0.01em;
        }
        button {
            padding:10px 16px;
            font-size:16px;
            cursor:pointer;
            border:1px solid #666;
            background:#2d2d2d;
            color:#f1f1f1;
            border-radius:8px;
        }
        select {
            padding:8px 10px;
            font-size:15px;
            border-radius:8px;
            border:1px solid #666;
            background:#2a2a2a;
            color:#f1f1f1;
            min-width:120px;
        }
        #label {
            display:inline-block;
            min-width:120px;
            font-weight:700;
            letter-spacing:0.03em;
        }
        .bottom-controls {
            position:fixed;
            left:0;
            right:0;
            bottom:0;
            background:#141414;
            border-top:1px solid #3c3c3c;
            padding:10px 10px calc(12px + env(safe-area-inset-bottom));
            box-shadow:0 -6px 16px rgba(0, 0, 0, 0.35);
        }
        .row {
            max-width:1240px;
            margin:0 auto;
            display:flex;
            align-items:center;
            justify-content:center;
            flex-wrap:wrap;
            gap:10px;
        }
        #hourSlider {
            width:min(960px, 94vw);
            height:34px;
            touch-action:pan-x;
        }
        @media (max-width: 760px) {
            #title { font-size:20px; margin:8px 0 10px; }
            button { font-size:14px; padding:8px 12px; }
            select { font-size:14px; min-width:96px; }
            #label { min-width:86px; }
            .bottom-controls { padding:10px 8px 12px; }
            .row { gap:8px; }
            #hourSlider { width:min(960px, 96vw); }
            .map-wrap {
                height:52vh;
                min-height:160px;
            }
        }
    </style>
</head>
<body>
    <div class="wrap">
        <h2 id="title">WeatherNext2 viewer</h2>
        <div class="map-wrap">
            <img id="map" src="" alt="WN2 map">
        </div>
    </div>

    <div class="bottom-controls">
        <div class="row">
            <label for="run">Run:</label>
            <select id="run"></select>
            <label for="product">Map:</label>
            <select id="product"></select>
            <label for="snowRatio">Snow Ratio:</label>
            <select id="snowRatio"></select>
        </div>
        <div class="row" style="margin-top:8px;">
            <button id="prevBtn">Prev</button>
            <span id="label">Hour ---</span>
            <button id="nextBtn">Next</button>
        </div>
        <div class="row" style="margin-top:6px;">
            <input type="range" id="hourSlider" min="0" max="0" step="1" value="0">
        </div>
    </div>

    <script>
        const manifest = __MANIFEST_JSON__;
        const runs = Array.isArray(manifest.runs) ? manifest.runs : [];
        const snowProducts = new Set(Array.isArray(manifest.snow_products) ? manifest.snow_products : []);
        const productLabels = (manifest.product_labels && typeof manifest.product_labels === 'object') ? manifest.product_labels : {};
        let activeHours = [];
        let idx = 0;

        const mapEl = document.getElementById('map');
        const titleEl = document.getElementById('title');
        const mapWrapEl = document.querySelector('.map-wrap');
        const controlsEl = document.querySelector('.bottom-controls');
        const rootEl = document.documentElement;
        const labelEl = document.getElementById('label');
        const runEl = document.getElementById('run');
        const productEl = document.getElementById('product');
        const ratioEl = document.getElementById('snowRatio');
        const sliderEl = document.getElementById('hourSlider');
        const prevBtn = document.getElementById('prevBtn');
        const nextBtn = document.getElementById('nextBtn');

        function normalizeIntList(values, minValue, maxValue) {
            if (!Array.isArray(values)) return [];
            const out = [];
            for (const raw of values) {
                const n = Number(raw);
                if (!Number.isInteger(n)) continue;
                if (minValue !== null && n < minValue) continue;
                if (maxValue !== null && n > maxValue) continue;
                out.push(n);
            }
            return Array.from(new Set(out)).sort((a, b) => a - b);
        }

        function getCurrentRun() {
            const selected = runEl.value;
            for (const run of runs) {
                if (String(run.id) === selected) return run;
            }
            return runs.length ? runs[0] : null;
        }

        function setSelectOptions(selectEl, values, labelFn) {
            const prev = selectEl.value;
            selectEl.innerHTML = '';
            for (const value of values) {
                const option = document.createElement('option');
                option.value = String(value);
                option.textContent = labelFn(value);
                selectEl.appendChild(option);
            }
            for (const value of values) {
                if (String(value) === prev) {
                    selectEl.value = prev;
                    return;
                }
            }
            if (values.length) {
                selectEl.value = String(values[0]);
            }
        }

        function buildFrameName(product, hour, ratio) {
            const hourStr = String(hour).padStart(3, '0');
            if (snowProducts.has(product)) {
                const ratioStr = String(ratio).padStart(2, '0');
                return product + '_r' + ratioStr + '_' + hourStr + '.jpg';
            }
            return product + '_' + hourStr + '.jpg';
        }

        function syncRunScopedControls() {
            const run = getCurrentRun();
            if (!run) {
                activeHours = [];
                productEl.innerHTML = '';
                ratioEl.innerHTML = '';
                sliderEl.max = '0';
                sliderEl.value = '0';
                idx = 0;
                return;
            }
            const products = Array.isArray(run.products) ? run.products : [];
            const ratios = normalizeIntList(run.snow_ratios, 10, 20);
            const hours = normalizeIntList(run.hours, 0, null);
            setSelectOptions(productEl, products, (key) => productLabels[key] || key);
            setSelectOptions(ratioEl, ratios.length ? ratios : [10], (ratio) => ratio + ':1');

            const previousHour = activeHours[idx];
            activeHours = hours;
            if (activeHours.length === 0) {
                idx = 0;
            } else {
                const found = activeHours.indexOf(previousHour);
                idx = found >= 0 ? found : 0;
            }
            sliderEl.max = String(Math.max(0, activeHours.length - 1));
            sliderEl.value = String(idx);
        }

        function viewportHeight() {
            if (window.visualViewport && Number.isFinite(window.visualViewport.height)) {
                return Math.floor(window.visualViewport.height);
            }
            return Math.floor(window.innerHeight || document.documentElement.clientHeight || 800);
        }

        function syncBottomInset() {
            const h = controlsEl ? Math.ceil(controlsEl.getBoundingClientRect().height) : 0;
            rootEl.style.setProperty('--controls-h', String(Math.max(110, h)) + 'px');
            document.body.style.paddingBottom = String(h + 14) + 'px';
            if (mapWrapEl) {
                const isMobile = window.matchMedia('(max-width: 760px)').matches;
                const mapTop = Math.max(0, Math.ceil(mapWrapEl.getBoundingClientRect().top));
                const vh = Math.max(320, viewportHeight());
                const edgeGap = isMobile ? 8 : 12;
                const minMapHeight = isMobile ? 160 : 220;
                const targetHeight = Math.max(minMapHeight, vh - h - mapTop - edgeGap);
                mapWrapEl.style.height = String(targetHeight) + 'px';
            }
        }

        function render() {
            const run = getCurrentRun();
            if (!run || !activeHours.length || !productEl.value) {
                mapEl.src = '';
                labelEl.innerText = 'No hours';
                titleEl.innerText = 'WeatherNext2 viewer';
                syncBottomInset();
                return;
            }

            const product = productEl.value;
            const isSnow = snowProducts.has(product);
            ratioEl.disabled = !isSnow;
            if (!isSnow) {
                ratioEl.title = 'Snow ratio applies to snowfall accumulation maps only.';
            } else {
                ratioEl.title = '';
            }

            const hour = activeHours[idx];
            const ratio = Number(ratioEl.value || 10);
            const frameName = buildFrameName(product, hour, ratio);
            const runId = String(run.id);
            const hourStr = String(hour).padStart(3, '0');

            mapEl.src = 'runs/' + runId + '/' + frameName;
            mapEl.alt = runId + ' ' + product + ' hour ' + hourStr;
            labelEl.innerText = 'Hour ' + hourStr;
            titleEl.innerText = 'WeatherNext2 viewer';
            syncBottomInset();
        }

        function change(dir) {
            if (!activeHours.length) return;
            idx = (idx + dir + activeHours.length) % activeHours.length;
            sliderEl.value = String(idx);
            render();
        }

        runEl.addEventListener('change', () => {
            syncRunScopedControls();
            render();
        });
        productEl.addEventListener('change', render);
        ratioEl.addEventListener('change', render);
        sliderEl.addEventListener('input', () => {
            idx = Number(sliderEl.value);
            render();
        });
        prevBtn.addEventListener('click', () => change(-1));
        nextBtn.addEventListener('click', () => change(1));

        setSelectOptions(runEl, runs.map((run) => run.id), (id) => {
            const run = runs.find((item) => String(item.id) === String(id));
            return run ? (run.label || String(id)) : String(id);
        });
        if (runs.length) {
            const defaultId = runs.some((run) => String(run.id) === String(manifest.default_run_id))
                ? String(manifest.default_run_id)
                : String(runs[0].id);
            runEl.value = defaultId;
        }
        window.addEventListener('resize', syncBottomInset);
        window.addEventListener('orientationchange', syncBottomInset);
        if (window.visualViewport) {
            window.visualViewport.addEventListener('resize', syncBottomInset);
        }
        syncRunScopedControls();
        syncBottomInset();
        render();
    </script>
</body>
</html>
"""
html = html_template.replace('__MANIFEST_JSON__', manifest_json)

with open(f'{OUTPUT}/index.html', 'w', encoding='utf-8') as f:
    f.write(html)

