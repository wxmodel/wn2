import ee
import os
import json
import time
import glob
import hashlib
import math
import re
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
product_mode_env = os.environ.get('WN2_PRODUCT_MODE')
hour_shard_index_env = os.environ.get('WN2_HOUR_SHARD_INDEX')
hour_shard_total_env = os.environ.get('WN2_HOUR_SHARD_TOTAL')
resume_existing_env = os.environ.get('WN2_RESUME_EXISTING')
skip_cleanup_run_dir_env = os.environ.get('WN2_SKIP_CLEANUP_RUN_DIR')
allow_no_products_env = os.environ.get('WN2_ALLOW_NO_PRODUCTS')
adaptive_long_range_env = os.environ.get('WN2_ADAPTIVE_LONG_RANGE')
long_range_threshold_env = os.environ.get('WN2_LONG_RANGE_THRESHOLD')
min_valid_frame_bytes_env = os.environ.get('WN2_MIN_VALID_FRAME_BYTES')
climo_window_days_env = os.environ.get('WN2_CLIMO_DOY_WINDOW_DAYS')
short_range_accuracy_hours_env = os.environ.get('WN2_SHORT_RANGE_ACCURACY_HOURS')
run_nh_z500a_env = os.environ.get('WN2_RUN_NH_Z500A')
run_na_z500a_env = os.environ.get('WN2_RUN_NA_Z500A')
run_conus_mslp_ptype_env = os.environ.get('WN2_RUN_CONUS_MSLP_PTYPE')
run_ne_mslp_ptype_env = os.environ.get('WN2_RUN_NE_MSLP_PTYPE')
run_conus_vort500_env = os.environ.get('WN2_RUN_CONUS_VORT500')
run_conus_snow_accum_env = os.environ.get('WN2_RUN_CONUS_SNOW_ACCUM')
run_ne_snow_accum_env = os.environ.get('WN2_RUN_NE_SNOW_ACCUM')
run_ne_zoom_snow_accum_env = os.environ.get('WN2_RUN_NE_ZOOM_SNOW_ACCUM')
run_conus_t2m_env = os.environ.get('WN2_RUN_CONUS_T2M')
run_conus_t2m_anom_env = os.environ.get('WN2_RUN_CONUS_T2M_ANOM')
local_true_anom_render_env = os.environ.get('WN2_LOCAL_TRUE_ANOM_RENDER')


def _env_flag(raw, default=False):
    if raw is None:
        return default
    return str(raw).strip().lower() in ('1', 'true', 'yes', 'on')


def _select_product_flag(raw, default=True):
    if raw is None:
        return default
    return _env_flag(raw, default=default)


LOCAL_TRUE_ANOMALY_RENDER = _env_flag(local_true_anom_render_env, default=True)
RESUME_EXISTING = _env_flag(resume_existing_env, default=True)
SKIP_CLEANUP_RUN_DIR = _env_flag(skip_cleanup_run_dir_env, default=RESUME_EXISTING)
ALLOW_NO_PRODUCTS = _env_flag(allow_no_products_env, default=False)
ADAPTIVE_LONG_RANGE = _env_flag(adaptive_long_range_env, default=True)

FAST_RENDER = _env_flag(fast_render_env, default=(event_name == 'schedule'))
if FAST_RENDER:
    ANOMALY_DIMS = '1080x790'
    CONUS_DIMS = '1300x930'
    NE_DIMS = '1180x930'
    PTYPE_CONUS_DIMS = '1320x960'
    PTYPE_NE_DIMS = '1200x980'
    SNOW_CONUS_DIMS = '1320x960'
    SNOW_NE_DIMS = '1200x980'
    NH_SOURCE_DIMS = '1400x280'
    NH_POLAR_DIMS = 980
    ANOMALY_NA_SCALE_M = 52000
    ANOMALY_NH_SCALE_M = 76000
    ANOMALY_WORK_SCALE_M = 220000
    Z500_NH_ANOM_SCALES_M = [260000, 360000, 480000]
    Z500_NA_ANOM_SCALES_M = [200000, 300000, 420000]
    T2M_ANOM_WORK_SCALES_M = [70000, 110000, 150000]
    LOCAL_Z500_NH_SCALES_M = [130000, 180000, 240000]
    LOCAL_Z500_NA_SCALES_M = [110000, 150000, 210000]
    LOCAL_T2M_ANOM_SCALES_M = [45000, 65000, 95000]
else:
    ANOMALY_DIMS = '1200x880'
    CONUS_DIMS = '1400x1000'
    NE_DIMS = '1200x980'
    PTYPE_CONUS_DIMS = '1600x1140'
    PTYPE_NE_DIMS = '1400x1120'
    SNOW_CONUS_DIMS = '1600x1140'
    SNOW_NE_DIMS = '1400x1120'
    NH_SOURCE_DIMS = '1600x320'
    NH_POLAR_DIMS = 1080
    ANOMALY_NA_SCALE_M = 52000
    ANOMALY_NH_SCALE_M = 76000
    ANOMALY_WORK_SCALE_M = 240000
    Z500_NH_ANOM_SCALES_M = [240000, 340000, 460000]
    Z500_NA_ANOM_SCALES_M = [180000, 280000, 380000]
    T2M_ANOM_WORK_SCALES_M = [65000, 100000, 140000]
    LOCAL_Z500_NH_SCALES_M = [90000, 130000, 180000]
    LOCAL_Z500_NA_SCALES_M = [70000, 105000, 150000]
    LOCAL_T2M_ANOM_SCALES_M = [30000, 45000, 70000]

workers_env = os.environ.get('EXPORT_WORKERS')
try:
    EXPORT_WORKERS = int(workers_env) if workers_env else (2 if FAST_RENDER else 2)
except ValueError:
    EXPORT_WORKERS = 2
EXPORT_WORKERS = max(1, min(4, EXPORT_WORKERS))
print(f'[{ts()}] Render profile: fast={FAST_RENDER}, workers={EXPORT_WORKERS}, dims={ANOMALY_DIMS}/{CONUS_DIMS}.')

ANOMALY_PALETTE = [
    '#f057da', '#b23ccf', '#7244cf', '#4062db', '#2b85eb', '#2eaef1',
    '#39d6ff', '#29d652', '#84e86a',
    '#d9dde2',
    '#f4ee9a', '#f7c96d', '#f59a50', '#ee6537', '#dd3125', '#9b1416'
]
ANOMALY_NEG_PALETTE = ['#6f00a8', '#8f45c8', '#5f58cf', '#2f75e2', '#5fa8ef', '#9fd7f5']
ANOMALY_POS_PALETTE = ['#f6e48e', '#f8c06b', '#f39a55', '#ea6e45', '#d93f2f', '#9a1f16']
ANOMALY_MIN_M = -300
ANOMALY_MAX_M = 300
ANOMALY_NEUTRAL_M = 6
ANOMALY_DISPLAY_GAIN = 1.25
ANOMALY_SMOOTH_RADIUS_PX = 0
BASEMAP_LAND_COLOR = '#e6ebef'
BASEMAP_OCEAN_COLOR = '#d6dde4'
Z500_MINOR_CONTOUR_INTERVAL = 6
Z500_MAJOR_CONTOUR_INTERVAL = 12
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
T2M_F_PALETTE = [
    '#5a168a', '#3344b2', '#2f75d6', '#55a7eb', '#8fd1f4', '#cce8fa',
    '#f1f3ef',
    '#f7e5a3', '#f8cc72', '#f3a14f', '#ea6f3b', '#d33e2e', '#9e1f1f'
]
T2M_ANOM_F_PALETTE = [
    '#6f00a8', '#4b3fcb', '#2f73e0', '#58adef', '#9ad7f7',
    '#e6e8ea',
    '#f4e58c', '#f4c264', '#ef9849', '#e46535', '#ca2f24', '#8e1313'
]
T2M_F_MIN = -20.0
T2M_F_MAX = 110.0
T2M_ANOM_F_MIN = -40.0
T2M_ANOM_F_MAX = 40.0
CLIMO_H500_COLLECTION = (
    ee.ImageCollection(CLIMO_ASSET)
    .select(CLIMO_H500_BAND)
    .filter(ee.Filter.calendarRange(CLIMO_START_YEAR, CLIMO_END_YEAR, 'year'))
)
CLIMO_H500_CACHE = {}
CLIMO_T2M_COLLECTION = (
    ee.ImageCollection(CLIMO_ASSET)
    .select('T2M')
    .filter(ee.Filter.calendarRange(CLIMO_START_YEAR, CLIMO_END_YEAR, 'year'))
)
CLIMO_T2M_CACHE = {}
CLIMO_SIZE_LOGGED = set()
CLIMO_COUNT_CACHE = {}
LOCAL_CLIMO_ARRAY_CACHE = {}

PRODUCT_OPTIONS = [
    ('nh_z500a', 'NH 500mb Height Anomaly', 'nh_z500a_*.jpg', run_nh_z500a_env),
    ('na_z500a', 'North America 500mb Height Anomaly', 'na_z500a_*.jpg', run_na_z500a_env),
    ('conus_mslp_ptype', 'CONUS MSLP + P-Type', 'conus_mslp_ptype_*.jpg', run_conus_mslp_ptype_env),
    ('ne_mslp_ptype', 'Northeast MSLP + P-Type', 'ne_mslp_ptype_*.jpg', run_ne_mslp_ptype_env),
    ('conus_vort500', 'CONUS 500mb Vorticity', 'conus_vort500_*.jpg', run_conus_vort500_env),
    ('conus_t2m', 'USA Region 2m Temperature', 'conus_t2m_*.jpg', run_conus_t2m_env),
    ('conus_t2m_anom', 'USA Region 2m Temperature Anomaly', 'conus_t2m_anom_*.jpg', run_conus_t2m_anom_env),
    ('conus_snow_accum', 'CONUS Snowfall Accumulation', 'conus_snow_accum_*.jpg', run_conus_snow_accum_env),
    ('ne_snow_accum', 'Northeast Snowfall Accumulation', 'ne_snow_accum_*.jpg', run_ne_snow_accum_env),
    ('ne_zoom_snow_accum', 'New England Zoom Snowfall Accumulation', 'ne_zoom_snow_accum_*.jpg', run_ne_zoom_snow_accum_env),
]
SNOW_PRODUCT_KEYS = {'conus_snow_accum', 'ne_snow_accum', 'ne_zoom_snow_accum'}
PRODUCT_MODE = (str(product_mode_env or '').strip().lower() or 'all')
USE_CUSTOM_PRODUCT_SELECTION = (event_name == 'workflow_dispatch' and PRODUCT_MODE == 'custom')

ENABLED_PRODUCTS = []
for key, label, pattern, raw_flag in PRODUCT_OPTIONS:
    enabled = _select_product_flag(raw_flag, default=True) if USE_CUSTOM_PRODUCT_SELECTION else True
    if enabled:
        ENABLED_PRODUCTS.append((key, label, pattern))

if not ENABLED_PRODUCTS:
    if ALLOW_NO_PRODUCTS:
        print(f'[{ts()}] No products enabled for this shard; exiting early (WN2_ALLOW_NO_PRODUCTS=1).')
        raise SystemExit(0)
    raise ValueError(
        'No products selected. Enable at least one WN2_RUN_* product flag or select a checkbox in workflow_dispatch.'
    )

print(f'[{ts()}] Enabled products: {[k for k, _, _ in ENABLED_PRODUCTS]}')
if product_mode_env:
    print(f'[{ts()}] Workflow product mode: {PRODUCT_MODE} (custom_selection={USE_CUSTOM_PRODUCT_SELECTION})')


def cleanup_old_products():
    stale_patterns = [
        'z500a_*.jpg',
        'nh_z500a_*.jpg',
        'na_z500a_*.jpg',
        'conus_mslp_ptype_*.jpg',
        'ne_mslp_ptype_*.jpg',
        'conus_vort500_*.jpg',
        'conus_t2m_*.jpg',
        'conus_t2m_anom_*.jpg',
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


def cleanup_current_run_products():
    run_patterns = [
        'nh_z500a_*.jpg',
        'na_z500a_*.jpg',
        'conus_mslp_ptype_*.jpg',
        'ne_mslp_ptype_*.jpg',
        'conus_vort500_*.jpg',
        'conus_t2m_*.jpg',
        'conus_t2m_anom_*.jpg',
        'conus_snow_accum_*.jpg',
        'ne_snow_accum_*.jpg',
        'ne_zoom_snow_accum_*.jpg',
    ]
    removed = 0
    for pattern in run_patterns:
        for path in RUN_OUTPUT_DIR.glob(pattern):
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        print(f'[{ts()}] Cleared {removed} stale frame(s) from current run directory: {RUN_OUTPUT_DIR}')


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
    max_attempts = 2
    request_timeout_s = 120
    for attempt in range(1, max_attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=request_timeout_s) as response:
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
            if transient and attempt < max_attempts:
                wait_s = attempt * 3
                print(
                    f'[{ts()}] Retry {attempt}/{max_attempts - 1} for {out_path} '
                    f'after transient error: {msg}'
                )
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


def _parse_hour_shard(index_raw, total_raw):
    idx = _parse_int(index_raw)
    total = _parse_int(total_raw)
    if idx is None or total is None:
        return 0, 1
    total = max(1, total)
    idx = max(0, min(total - 1, idx))
    return idx, total


def _parse_snow_ratios(csv_value):
    if csv_value:
        parts = [p.strip() for p in str(csv_value).split(',') if p.strip()]
        parsed = sorted({v for v in (_parse_int(p) for p in parts) if v is not None and 10 <= v <= 20})
        if parsed:
            return parsed
    return [10]


HOUR_SHARD_INDEX, HOUR_SHARD_TOTAL = _parse_hour_shard(hour_shard_index_env, hour_shard_total_env)
long_range_threshold_raw = _parse_int(long_range_threshold_env)
if long_range_threshold_raw is None:
    LONG_RANGE_THRESHOLD = 120
else:
    LONG_RANGE_THRESHOLD = max(24, long_range_threshold_raw)
min_valid_frame_bytes_raw = _parse_int(min_valid_frame_bytes_env)
if min_valid_frame_bytes_raw is None:
    MIN_VALID_FRAME_BYTES = 12000
else:
    MIN_VALID_FRAME_BYTES = max(4000, min_valid_frame_bytes_raw)
climo_window_days_raw = _parse_int(climo_window_days_env)
if climo_window_days_raw is None:
    CLIMO_DOY_WINDOW_DAYS = 5
else:
    CLIMO_DOY_WINDOW_DAYS = max(0, min(15, climo_window_days_raw))
short_range_accuracy_hours_raw = _parse_int(short_range_accuracy_hours_env)
if short_range_accuracy_hours_raw is None:
    SHORT_RANGE_ACCURACY_HOURS = 24
else:
    SHORT_RANGE_ACCURACY_HOURS = max(0, min(72, short_range_accuracy_hours_raw))

SNOW_RATIOS = _parse_snow_ratios(snow_ratio_csv_env)
run_history_hours_raw = _parse_int(run_history_hours_env)
if run_history_hours_raw is None:
    RUN_HISTORY_HOURS = 24
else:
    RUN_HISTORY_HOURS = max(6, min(72, run_history_hours_raw))

print(f'[{ts()}] Snow ratios selected: {SNOW_RATIOS}')
print(f'[{ts()}] Run history retention: {RUN_HISTORY_HOURS}h')
print(
    f'[{ts()}] Resume existing frames: {RESUME_EXISTING} '
    f'(min_valid_bytes={MIN_VALID_FRAME_BYTES}, skip_cleanup={SKIP_CLEANUP_RUN_DIR})'
)
print(
    f'[{ts()}] Climatology baseline: MERRA2 {CLIMO_START_YEAR}-{CLIMO_END_YEAR} '
    f'(day-window=+/-{CLIMO_DOY_WINDOW_DAYS}d, not model-climate).'
)
if SHORT_RANGE_ACCURACY_HOURS > 0:
    print(f'[{ts()}] Short-range anomaly fidelity boost enabled through hour {SHORT_RANGE_ACCURACY_HOURS}.')
if ADAPTIVE_LONG_RANGE:
    print(f'[{ts()}] Adaptive long-range render enabled from hour {LONG_RANGE_THRESHOLD}+.')
if HOUR_SHARD_TOTAL > 1:
    print(f'[{ts()}] Hour sharding requested: shard {HOUR_SHARD_INDEX + 1}/{HOUR_SHARD_TOTAL}.')
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


def _apply_hour_shard(hours, shard_index=0, shard_total=1):
    if shard_total <= 1 or not hours:
        return list(hours)
    chunk_size = int(math.ceil(len(hours) / float(shard_total)))
    start = shard_index * chunk_size
    end = min(len(hours), start + chunk_size)
    if start >= len(hours):
        return []
    return list(hours[start:end])


AVAILABLE_HOURS = _infer_available_hours(latest_start_collection)
HOURS = _select_hours(AVAILABLE_HOURS)
if HOUR_SHARD_TOTAL > 1:
    base_hours = list(HOURS)
    HOURS = _apply_hour_shard(HOURS, shard_index=HOUR_SHARD_INDEX, shard_total=HOUR_SHARD_TOTAL)
    if HOURS:
        print(
            f'[{ts()}] Hour shard selection: {len(HOURS)} hour(s) '
            f'({HOURS[0]}..{HOURS[-1]}) from {len(base_hours)} total selected hours.'
        )
    else:
        print(
            f'[{ts()}] Hour shard selection produced no hours for '
            f'shard {HOUR_SHARD_INDEX + 1}/{HOUR_SHARD_TOTAL}; continuing with no-op render.'
        )

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
BORDER_CACHE_DIR = RUN_OUTPUT_DIR / '.cache' / 'borders'
BORDER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
BORDER_OVERLAY_CACHE = {}
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
def contour_overlay(field, interval, color, opacity=0.82, smooth_px=0, thicken_px=0, line_width_frac=0.010):
    # Draw thin, crisp contour lines by finding narrow zero-crossings at integer contour levels.
    smoothed = field.resample('bilinear')
    if smooth_px and smooth_px > 0:
        smoothed = smoothed.focalMean(int(smooth_px), 'circle', 'pixels')
    scaled = smoothed.divide(float(interval))
    dist = scaled.subtract(scaled.round()).abs()
    width = max(0.004, min(0.020, float(line_width_frac)))
    lines = dist.lte(width)
    if thicken_px and thicken_px > 0:
        lines = lines.focalMax(int(thicken_px))
    return lines.selfMask().visualize(palette=[color], opacity=opacity)


def highlight_iso_overlay(field, level, color='#2455ff', opacity=0.92, tolerance=1.2, smooth_px=0):
    smoothed = field.resample('bilinear')
    if smooth_px and smooth_px > 0:
        smoothed = smoothed.focalMean(int(smooth_px), 'circle', 'pixels')
    line = smoothed.subtract(float(level)).abs().lte(float(tolerance)).selfMask()
    return line.visualize(palette=[color], opacity=opacity)


def border_overlay(include_states=False, state_names=None, region_geom=None, detailed=True):
    country_fc = COUNTRIES_BORDERS if detailed else COUNTRIES
    if region_geom is not None:
        country_fc = country_fc.filterBounds(region_geom)
    country_lines = ee.Image().byte().paint(country_fc, 1, 1).selfMask().visualize(palette=['#333333'])
    if include_states:
        state_fc = US_STATES if not state_names else US_STATES.filter(ee.Filter.inList('NAME', state_names))
        if region_geom is not None:
            state_fc = state_fc.filterBounds(region_geom)
        state_lines = ee.Image().byte().paint(state_fc, 1, 1).selfMask().visualize(palette=['#6b4a2c'])
        return ee.ImageCollection([country_lines, state_lines]).mosaic()
    return country_lines


def basemap_overlay(region_geom, land_color='#ececec', ocean_color='#cfe0ea', land_fc=None):
    ocean = ee.Image.constant(1).clip(region_geom).visualize(palette=[ocean_color], opacity=1.0)
    land_features = land_fc if land_fc is not None else COUNTRIES
    if region_geom is not None:
        land_features = land_features.filterBounds(region_geom)
    land_mask = ee.Image().byte().paint(land_features, 1, 1).clip(region_geom).selfMask()
    land = land_mask.visualize(palette=[land_color], opacity=1.0)
    return ee.ImageCollection([ocean, land]).mosaic()


def flat_background_overlay(region_geom, color='#cfe0ea'):
    return ee.Image.constant(1).clip(region_geom).visualize(palette=[color], opacity=1.0)


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


def _clip_collection_to_region(collection, region_geom):
    if region_geom is None:
        return collection
    return ee.ImageCollection(collection.map(lambda im: ee.Image(im).clip(region_geom)))


def _coarsen_for_compute(image, scale_m, min_scale_m):
    img = ee.Image(image).toFloat()
    if scale_m is None:
        return img
    # Keep compute graph lightweight; output scale is controlled at export time.
    return img.resample('bilinear')


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


def z500_climo_1991_2020_m(valid_utc, region_geom=None, cache_tag='global', analysis_scale_m=None):
    doy = valid_utc.timetuple().tm_yday
    month = int(valid_utc.month)
    day = int(valid_utc.day)
    hour = int(valid_utc.hour)
    scale_key = int(max(25000, float(analysis_scale_m))) if analysis_scale_m is not None else None
    cache_key = (doy, hour, cache_tag, scale_key)
    cached = CLIMO_H500_CACHE.get(cache_key)
    if cached is not None:
        return cached

    hour_collection = CLIMO_H500_COLLECTION.filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    hour_collection = _clip_collection_to_region(hour_collection, region_geom)
    if CLIMO_DOY_WINDOW_DAYS <= 0:
        window_collection = hour_collection.filter(
            ee.Filter.calendarRange(month, month, 'month')
        ).filter(
            ee.Filter.calendarRange(day, day, 'day_of_month')
        )
    else:
        start_doy = doy - CLIMO_DOY_WINDOW_DAYS
        end_doy = doy + CLIMO_DOY_WINDOW_DAYS
        window_collection = hour_collection.filter(_wrap_day_of_year_filter(start_doy, end_doy))
    fallback_collection = CLIMO_H500_COLLECTION.filter(
        ee.Filter.calendarRange(month, month, 'month')
    ).filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    fallback_collection = _clip_collection_to_region(fallback_collection, region_geom)
    count_key = ('h500', month, day, hour)
    window_n = CLIMO_COUNT_CACHE.get(count_key)
    if window_n is None:
        window_n = int(window_collection.size().getInfo())
        CLIMO_COUNT_CACHE[count_key] = window_n
    size_log_key = ('h500', month, day, hour)
    if size_log_key not in CLIMO_SIZE_LOGGED:
        try:
            fallback_n = int(fallback_collection.size().getInfo())
            print(f'[{ts()}] H500 climo sample count month/day/hour={window_n}, fallback month/hour={fallback_n}.')
        except Exception as e:
            print(f'[{ts()}] H500 climo sample-count check skipped: {e}')
        CLIMO_SIZE_LOGGED.add(size_log_key)
    climo_source = window_collection if window_n > 0 else fallback_collection
    climo = ee.Image(climo_source.mean()).rename('z500_climo_m').resample('bilinear')
    if analysis_scale_m is not None:
        climo = _coarsen_for_compute(climo, analysis_scale_m, min_scale_m=25000)
    CLIMO_H500_CACHE[cache_key] = climo
    return climo


def z500_anomaly_m(img, hour, region_geom=None, cache_tag='global', analysis_scale_m=None):
    valid_utc = RUN_INIT_UTC + timedelta(hours=int(hour))
    forecast_height_m = img.select(WN2_Z500_BAND).divide(9.80665)
    climo_height_m = z500_climo_1991_2020_m(
        valid_utc,
        region_geom=region_geom,
        cache_tag=cache_tag,
        analysis_scale_m=analysis_scale_m,
    )
    if region_geom is not None:
        forecast_height_m = forecast_height_m.clip(region_geom)
        climo_height_m = climo_height_m.clip(region_geom)
    if analysis_scale_m is not None:
        forecast_height_m = _coarsen_for_compute(forecast_height_m, analysis_scale_m, min_scale_m=25000)
        climo_height_m = _coarsen_for_compute(climo_height_m, analysis_scale_m, min_scale_m=25000)
    else:
        forecast_height_m = forecast_height_m.toFloat()
        climo_height_m = climo_height_m.toFloat()
    # Integer-space subtraction lowers EE memory while preserving true anomalies.
    forecast_dm = forecast_height_m.divide(10.0).round().toInt16()
    climo_dm = climo_height_m.divide(10.0).round().toInt16()
    return forecast_dm.subtract(climo_dm).multiply(10.0).toFloat().rename('z500_anomaly_m')


def t2m_climo_1991_2020_c(valid_utc, region_geom=None, cache_tag='global', analysis_scale_m=None):
    doy = valid_utc.timetuple().tm_yday
    month = int(valid_utc.month)
    day = int(valid_utc.day)
    hour = int(valid_utc.hour)
    scale_key = int(max(15000, float(analysis_scale_m))) if analysis_scale_m is not None else None
    cache_key = (doy, hour, cache_tag, scale_key)
    cached = CLIMO_T2M_CACHE.get(cache_key)
    if cached is not None:
        return cached

    hour_collection = CLIMO_T2M_COLLECTION.filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    hour_collection = _clip_collection_to_region(hour_collection, region_geom)
    if CLIMO_DOY_WINDOW_DAYS <= 0:
        window_collection = hour_collection.filter(
            ee.Filter.calendarRange(month, month, 'month')
        ).filter(
            ee.Filter.calendarRange(day, day, 'day_of_month')
        )
    else:
        start_doy = doy - CLIMO_DOY_WINDOW_DAYS
        end_doy = doy + CLIMO_DOY_WINDOW_DAYS
        window_collection = hour_collection.filter(_wrap_day_of_year_filter(start_doy, end_doy))
    fallback_collection = CLIMO_T2M_COLLECTION.filter(
        ee.Filter.calendarRange(month, month, 'month')
    ).filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    fallback_collection = _clip_collection_to_region(fallback_collection, region_geom)
    count_key = ('t2m', month, day, hour)
    window_n = CLIMO_COUNT_CACHE.get(count_key)
    if window_n is None:
        window_n = int(window_collection.size().getInfo())
        CLIMO_COUNT_CACHE[count_key] = window_n
    size_log_key = ('t2m', month, day, hour)
    if size_log_key not in CLIMO_SIZE_LOGGED:
        try:
            fallback_n = int(fallback_collection.size().getInfo())
            print(f'[{ts()}] T2M climo sample count month/day/hour={window_n}, fallback month/hour={fallback_n}.')
        except Exception as e:
            print(f'[{ts()}] T2M climo sample-count check skipped: {e}')
        CLIMO_SIZE_LOGGED.add(size_log_key)
    climo_source = window_collection if window_n > 0 else fallback_collection
    climo_k = ee.Image(climo_source.mean()).rename('t2m_climo_k').resample('bilinear')
    if analysis_scale_m is not None:
        climo_k = _coarsen_for_compute(climo_k, analysis_scale_m, min_scale_m=15000)
    climo_c = climo_k.subtract(273.15).rename('t2m_climo_c')
    if region_geom is not None:
        climo_c = climo_c.clip(region_geom)
    CLIMO_T2M_CACHE[cache_key] = climo_c
    return climo_c


def t2m_anomaly_c(img, hour, region_geom=None, cache_tag='global', analysis_scale_m=None):
    valid_utc = RUN_INIT_UTC + timedelta(hours=int(hour))
    forecast_t2m_c = img.select(WN2_T2M_BAND).subtract(273.15)
    climo_t2m_c = t2m_climo_1991_2020_c(
        valid_utc,
        region_geom=region_geom,
        cache_tag=cache_tag,
        analysis_scale_m=analysis_scale_m,
    )
    if region_geom is not None:
        forecast_t2m_c = forecast_t2m_c.clip(region_geom)
        climo_t2m_c = climo_t2m_c.clip(region_geom)
    if analysis_scale_m is not None:
        forecast_t2m_c = _coarsen_for_compute(forecast_t2m_c, analysis_scale_m, min_scale_m=15000)
        climo_t2m_c = _coarsen_for_compute(climo_t2m_c, analysis_scale_m, min_scale_m=15000)
    else:
        forecast_t2m_c = forecast_t2m_c.toFloat()
        climo_t2m_c = climo_t2m_c.toFloat()
    forecast_tc10 = forecast_t2m_c.multiply(10.0).round().toInt16()
    climo_tc10 = climo_t2m_c.multiply(10.0).round().toInt16()
    return forecast_tc10.subtract(climo_tc10).divide(10.0).toFloat().rename('t2m_anomaly_c')


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


def split_dimensions_horizontal(source_dims):
    if isinstance(source_dims, str) and 'x' in source_dims:
        w_str, h_str = source_dims.lower().split('x', 1)
        try:
            w = int(w_str)
            h = int(h_str)
            return f'{max(360, w // 2)}x{max(280, h)}'
        except ValueError:
            return source_dims
    if isinstance(source_dims, int):
        return max(360, int(source_dims // 2))
    return source_dims


def split_dimensions_horizontal_parts(source_dims, parts=2):
    parts = max(1, int(parts))
    if parts == 1:
        return source_dims
    if isinstance(source_dims, str) and 'x' in source_dims:
        w_str, h_str = source_dims.lower().split('x', 1)
        try:
            w = int(w_str)
            h = int(h_str)
            return f'{max(220, w // parts)}x{max(280, h)}'
        except ValueError:
            return source_dims
    if isinstance(source_dims, int):
        return max(220, int(source_dims // parts))
    return source_dims


def split_region_longitude(region, parts=2):
    min_lon, min_lat, max_lon, max_lat = [float(v) for v in region]
    parts = max(1, int(parts))
    span = max_lon - min_lon
    if parts == 1 or span <= 0:
        return [[min_lon, min_lat, max_lon, max_lat]]
    bounds = []
    for i in range(parts):
        lon0 = min_lon + span * (i / parts)
        lon1 = min_lon + span * ((i + 1) / parts)
        bounds.append([lon0, min_lat, lon1, max_lat])
    return bounds


def split_region_west_east(region):
    min_lon, min_lat, max_lon, max_lat = [float(v) for v in region]
    mid_lon = (min_lon + max_lon) * 0.5
    west = [min_lon, min_lat, mid_lon, max_lat]
    east = [mid_lon, min_lat, max_lon, max_lat]
    return west, east


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


def parse_dimensions(dimensions, fallback_w=1200, fallback_h=900):
    if isinstance(dimensions, int):
        w = max(200, int(dimensions))
        return w, max(200, int(fallback_h))
    if isinstance(dimensions, str) and 'x' in dimensions:
        w_str, h_str = dimensions.lower().split('x', 1)
        try:
            return max(200, int(w_str)), max(200, int(h_str))
        except ValueError:
            return fallback_w, fallback_h
    return fallback_w, fallback_h


def scale_dimensions(dimensions, factor=1.0, min_w=500, min_h=400):
    factor = float(max(0.2, min(2.0, factor)))
    if isinstance(dimensions, int):
        return max(min_w, int(round(dimensions * factor)))
    if isinstance(dimensions, str) and 'x' in dimensions:
        w_str, h_str = dimensions.lower().split('x', 1)
        try:
            w = max(min_w, int(round(int(w_str) * factor)))
            h = max(min_h, int(round(int(h_str) * factor)))
            return f'{w}x{h}'
        except ValueError:
            return dimensions
    return dimensions


def is_long_range_hour(hour):
    try:
        h = int(hour)
    except (TypeError, ValueError):
        return False
    return ADAPTIVE_LONG_RANGE and h >= LONG_RANGE_THRESHOLD


def adaptive_dimensions_for_hour(dimensions, hour, long_factor=0.90, min_w=500, min_h=400):
    if not is_long_range_hour(hour):
        return dimensions
    return scale_dimensions(dimensions, factor=long_factor, min_w=min_w, min_h=min_h)


def adaptive_interval_for_hour(base_interval, hour, long_interval=None, multiplier=1.3):
    if not is_long_range_hour(hour):
        return int(base_interval)
    if long_interval is not None:
        return int(long_interval)
    return max(int(base_interval) + 1, int(round(float(base_interval) * float(multiplier))))


def _is_memory_or_invalid_argument_error(msg):
    text = str(msg)
    return (
        'User memory limit exceeded' in text
        or 'INVALID_ARGUMENT' in text
        or 'Invalid argument' in text
    )


def _sample_rect_array(field_img, band_name, bounds, base_scale_m, max_attempts=4, context=''):
    import numpy as np

    geom = ee.Geometry.Rectangle(bounds, geodesic=False)
    current_scale = int(max(20000, float(base_scale_m)))
    fill_value = -9999.0
    last_msg = ''

    for _attempt in range(1, max_attempts + 1):
        try:
            sampled = (
                ee.Image(field_img)
                .select(band_name)
                .toFloat()
                .resample('bilinear')
                .reproject(crs=TARGET_CRS, scale=current_scale)
                .clip(geom)
            )
            info = sampled.sampleRectangle(region=geom, defaultValue=fill_value).getInfo()
            raw = (info or {}).get('properties', {}).get(band_name)
            if raw is None:
                raise RuntimeError(f'Sampled rectangle missing band "{band_name}".')
            arr = np.array(raw, dtype=np.float32)
            if arr.ndim != 2 or arr.size == 0:
                raise RuntimeError(f'Invalid sampled array shape for "{band_name}": {arr.shape}')
            arr[arr <= (fill_value + 0.5)] = np.nan
            # Normalize to north-up rows for plotting.
            if arr.shape[0] > 1:
                top_row = arr[0, :]
                bottom_row = arr[-1, :]
                if np.isfinite(top_row).any() and np.isfinite(bottom_row).any():
                    top = float(np.nanmean(top_row))
                    bot = float(np.nanmean(bottom_row))
                    if top < bot:
                        arr = np.flipud(arr)
            return arr, current_scale
        except Exception as e:
            last_msg = str(e)
            if _attempt < max_attempts and _is_memory_or_invalid_argument_error(last_msg):
                current_scale = int(current_scale * 1.55)
                continue
            label = context or f'band {band_name}'
            raise RuntimeError(
                f'sampleRectangle failed for {label} at scale {current_scale}m: {last_msg}'
            ) from e

    label = context or f'band {band_name}'
    raise RuntimeError(
        f'sampleRectangle failed for {label} after retries. Last error: {last_msg}'
    )


def _select_climo_source_collection(base_collection, valid_utc, region_geom, count_cache_prefix):
    month = int(valid_utc.month)
    day = int(valid_utc.day)
    hour = int(valid_utc.hour)
    doy = valid_utc.timetuple().tm_yday

    hour_collection = base_collection.filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    hour_collection = _clip_collection_to_region(hour_collection, region_geom)
    if CLIMO_DOY_WINDOW_DAYS <= 0:
        window_collection = hour_collection.filter(
            ee.Filter.calendarRange(month, month, 'month')
        ).filter(
            ee.Filter.calendarRange(day, day, 'day_of_month')
        )
    else:
        start_doy = doy - CLIMO_DOY_WINDOW_DAYS
        end_doy = doy + CLIMO_DOY_WINDOW_DAYS
        window_collection = hour_collection.filter(_wrap_day_of_year_filter(start_doy, end_doy))

    fallback_collection = base_collection.filter(
        ee.Filter.calendarRange(month, month, 'month')
    ).filter(ee.Filter.calendarRange(hour, hour, 'hour'))
    fallback_collection = _clip_collection_to_region(fallback_collection, region_geom)

    count_key = (count_cache_prefix, month, day, hour)
    window_n = CLIMO_COUNT_CACHE.get(count_key)
    if window_n is None:
        window_n = int(window_collection.size().getInfo())
        CLIMO_COUNT_CACHE[count_key] = window_n
    size_log_key = (count_cache_prefix, month, day, hour)
    if size_log_key not in CLIMO_SIZE_LOGGED:
        try:
            fallback_n = int(fallback_collection.size().getInfo())
            print(
                f'[{ts()}] {count_cache_prefix.upper()} climo sample count '
                f'month/day/hour={window_n}, fallback month/hour={fallback_n}.'
            )
        except Exception as e:
            print(f'[{ts()}] {count_cache_prefix.upper()} climo sample-count check skipped: {e}')
        CLIMO_SIZE_LOGGED.add(size_log_key)
    return window_collection if window_n > 0 else fallback_collection


def _sample_grouped_climo_array(
    source_collection,
    band_name,
    bounds,
    base_scale_m,
    group_years=3,
    context='',
):
    import numpy as np

    if group_years < 1:
        group_years = 1
    start_year = int(CLIMO_START_YEAR)
    end_year = int(CLIMO_END_YEAR)
    if end_year < start_year:
        raise RuntimeError(f'Invalid climatology year range: {start_year}-{end_year}')

    weighted_sum = None
    weight_sum = None
    current_scale = int(max(20000, float(base_scale_m)))
    groups_used = 0
    years_used = 0
    for y0 in range(start_year, end_year + 1, group_years):
        y1 = min(end_year, y0 + group_years - 1)
        grp = source_collection.filter(ee.Filter.calendarRange(y0, y1, 'year'))
        grp_n = int(grp.size().getInfo())
        if grp_n <= 0:
            continue
        groups_used += 1
        years_used += grp_n
        grp_img = ee.Image(grp.mean()).select(band_name).toFloat()
        grp_arr, used_scale = _sample_rect_array(
            grp_img,
            band_name,
            bounds,
            current_scale,
            context=f'{context} climo_{y0}_{y1}',
        )
        current_scale = max(current_scale, int(used_scale))
        if weighted_sum is None:
            weighted_sum = np.zeros_like(grp_arr, dtype=np.float64)
            weight_sum = np.zeros_like(grp_arr, dtype=np.float64)
        if weighted_sum.shape != grp_arr.shape:
            weighted_sum, grp_arr = _align_arrays(weighted_sum, grp_arr)
            weight_sum, _ = _align_arrays(weight_sum, grp_arr)
        valid = np.isfinite(grp_arr)
        if np.any(valid):
            weighted_sum[valid] += grp_arr[valid] * float(grp_n)
            weight_sum[valid] += float(grp_n)

    if weighted_sum is None or groups_used == 0 or years_used == 0:
        raise RuntimeError(f'{context} grouped climo sampling returned no data.')

    with np.errstate(invalid='ignore', divide='ignore'):
        out = (weighted_sum / weight_sum).astype(np.float32)
    out[~np.isfinite(out)] = np.nan
    return out, current_scale


def _align_arrays(a, b):
    import numpy as np

    a_np = np.asarray(a, dtype=np.float32)
    b_np = np.asarray(b, dtype=np.float32)
    h = min(a_np.shape[0], b_np.shape[0])
    w = min(a_np.shape[1], b_np.shape[1])
    return a_np[:h, :w], b_np[:h, :w]


def _fill_nan_gaps(field, fill_value=0.0):
    import numpy as np

    arr = np.asarray(field, dtype=np.float32).copy()
    if arr.ndim != 2 or not np.isnan(arr).any():
        return arr

    x_idx = np.arange(arr.shape[1], dtype=np.float32)
    for r in range(arr.shape[0]):
        row = arr[r, :]
        valid = np.isfinite(row)
        if valid.all():
            continue
        if valid.any():
            row[~valid] = np.interp(x_idx[~valid], x_idx[valid], row[valid]).astype(np.float32)
        else:
            row[:] = float(fill_value)
        arr[r, :] = row

    y_idx = np.arange(arr.shape[0], dtype=np.float32)
    for c in range(arr.shape[1]):
        col = arr[:, c]
        valid = np.isfinite(col)
        if valid.all():
            continue
        if valid.any():
            col[~valid] = np.interp(y_idx[~valid], y_idx[valid], col[valid]).astype(np.float32)
        else:
            col[:] = float(fill_value)
        arr[:, c] = col

    arr[~np.isfinite(arr)] = float(fill_value)
    return arr


def _contour_levels(field, interval):
    import numpy as np

    if not interval or interval <= 0:
        return None
    finite = field[np.isfinite(field)]
    if finite.size == 0:
        return None
    vmin = float(np.nanmin(finite))
    vmax = float(np.nanmax(finite))
    if not math.isfinite(vmin) or not math.isfinite(vmax) or vmax <= vmin:
        return None
    start = math.floor(vmin / float(interval)) * float(interval)
    end = math.ceil(vmax / float(interval)) * float(interval)
    levels = np.arange(start, end + float(interval) * 0.5, float(interval), dtype=np.float32)
    if levels.size > 120:
        step = max(1, int(math.ceil(levels.size / 120.0)))
        levels = levels[::step]
    return levels


def _render_local_anomaly_tile(
    anomaly_field,
    contour_field,
    bounds,
    out_file,
    width,
    height,
    palette,
    vmin,
    vmax,
    minor_interval=None,
    major_interval=12,
    minor_color='#2a2a2a',
    major_color='#1a1a1a',
    major_lw=1.0,
    highlight_level=None,
    highlight_color='#2455ff',
):
    import numpy as np
    import matplotlib

    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    anomaly_arr = np.asarray(anomaly_field, dtype=np.float32)
    contour_arr = np.asarray(contour_field, dtype=np.float32)
    if anomaly_arr.shape != contour_arr.shape:
        anomaly_arr, contour_arr = _align_arrays(anomaly_arr, contour_arr)
    anomaly_arr = _fill_nan_gaps(anomaly_arr, fill_value=0.0)
    contour_arr = _fill_nan_gaps(contour_arr, fill_value=float(np.nanmean(contour_arr)) if np.isfinite(contour_arr).any() else 540.0)

    anomaly_arr = np.clip(anomaly_arr * float(ANOMALY_DISPLAY_GAIN), float(vmin), float(vmax))
    lon_w, lat_s, lon_e, lat_n = [float(v) for v in bounds]
    x = np.linspace(lon_w, lon_e, anomaly_arr.shape[1], dtype=np.float32)
    y = np.linspace(lat_n, lat_s, anomaly_arr.shape[0], dtype=np.float32)

    dpi = 100
    fig = plt.figure(figsize=(max(2, width) / dpi, max(2, height) / dpi), dpi=dpi, frameon=False)
    ax = fig.add_axes([0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    ax.set_xlim(lon_w, lon_e)
    ax.set_ylim(lat_s, lat_n)
    ax.set_facecolor(BASEMAP_OCEAN_COLOR)

    cmap = LinearSegmentedColormap.from_list('wn2_anom', list(palette), N=256)
    ax.imshow(
        anomaly_arr,
        extent=[lon_w, lon_e, lat_s, lat_n],
        origin='upper',
        cmap=cmap,
        vmin=float(vmin),
        vmax=float(vmax),
        interpolation='bilinear',
        aspect='auto',
    )

    minor_levels = _contour_levels(contour_arr, minor_interval)
    if minor_levels is not None and major_interval and float(minor_interval) < float(major_interval):
        ax.contour(x, y, contour_arr, levels=minor_levels, colors=minor_color, linewidths=0.55, alpha=0.60)

    major_levels = _contour_levels(contour_arr, major_interval)
    if major_levels is not None:
        ax.contour(x, y, contour_arr, levels=major_levels, colors=major_color, linewidths=major_lw, alpha=0.92)

    if highlight_level is not None:
        try:
            ax.contour(
                x,
                y,
                contour_arr,
                levels=[float(highlight_level)],
                colors=highlight_color,
                linewidths=max(0.9, major_lw + 0.1),
                alpha=0.95,
            )
        except Exception:
            pass

    fig.savefig(
        out_file,
        format='jpg',
        dpi=dpi,
        bbox_inches='tight',
        pad_inches=0,
        pil_kwargs={'quality': 97, 'subsampling': 0},
    )
    plt.close(fig)


def _overlay_png_on_jpg(base_jpg, overlay_png):
    from PIL import Image

    with Image.open(base_jpg).convert('RGBA') as base, Image.open(overlay_png).convert('RGBA') as overlay:
        if overlay.size != base.size:
            overlay = overlay.resize(base.size, Image.BILINEAR)
        merged = Image.alpha_composite(base, overlay)
        merged.convert('RGB').save(base_jpg, format='JPEG', quality=97, subsampling=0)


def _export_border_overlay_png(bounds, out_png, width, height, include_states=False, detailed=False):
    region_geom = ee.Geometry.Rectangle(bounds, geodesic=False)
    border_img = border_overlay(
        include_states=include_states,
        region_geom=region_geom,
        detailed=detailed,
    )
    download_thumb(
        border_img,
        out_png,
        {
            'region': bounds,
            'format': 'png',
            'dimensions': f'{int(width)}x{int(height)}',
            'crs': TARGET_CRS,
        },
    )


def get_cached_border_overlay_png(bounds, width, height, include_states=False, detailed=False):
    key_payload = {
        'bounds': [round(float(v), 4) for v in bounds],
        'width': int(width),
        'height': int(height),
        'include_states': bool(include_states),
        'detailed': bool(detailed),
    }
    key_text = json.dumps(key_payload, sort_keys=True, separators=(',', ':'))
    key = hashlib.md5(key_text.encode('utf-8')).hexdigest()
    cached = BORDER_OVERLAY_CACHE.get(key)
    if cached and os.path.exists(cached):
        return cached

    out_png = str(BORDER_CACHE_DIR / f'border_{key}.png')
    if not os.path.exists(out_png):
        _export_border_overlay_png(
            bounds,
            out_png,
            width,
            height,
            include_states=include_states,
            detailed=detailed,
        )
    BORDER_OVERLAY_CACHE[key] = out_png
    return out_png


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
            [-240, -180, -120, -60, 0, 60, 120, 180, 240],
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

    if product_key == 'conus_t2m':
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '2m Temperature (degF) | USA Region (CONUS)', fill=(22, 22, 22), font=label_font)
        _draw_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, T2M_F_PALETTE)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [-20, 0, 10, 20, 32, 40, 50, 60, 70, 80, 90, 100, 110], T2M_F_MIN, T2M_F_MAX, tick_font)
        return

    if product_key == 'conus_t2m_anom':
        _draw_panel(draw, bar_x - 14, y - 4, bar_x + bar_w + 14, y + 72)
        draw.text((bar_x, y + 2), '2m Temperature Anomaly (degF) vs 1991-2020 | USA Region (CONUS)', fill=(22, 22, 22), font=label_font)
        _draw_gradient_bar(draw, bar_x, bar_y, bar_w, bar_h, T2M_ANOM_F_PALETTE)
        _draw_ticks(draw, bar_x, bar_y, bar_w, bar_h, [-40, -30, -20, -10, 0, 10, 20, 30, 40], T2M_ANOM_F_MIN, T2M_ANOM_F_MAX, tick_font)
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
        'conus_t2m': 'WN2 0.25 deg | 2m Temperature (degF) | USA Region (CONUS)',
        'conus_t2m_anom': 'WN2 0.25 deg | 2m Temperature Anomaly (degF) vs 1991-2020 | USA Region (CONUS)',
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
        elif product_key in (
            'nh_z500a',
            'na_z500a',
            'conus_vort500',
            'conus_t2m',
            'conus_t2m_anom',
            'conus_snow_accum',
            'ne_snow_accum',
            'ne_zoom_snow_accum',
        ):
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


def remap_nh_to_polar(
    out_file,
    lon0=NH_LON0,
    lat_min=20.0,
    lat_max=88.5,
    lon_w=NH_SOURCE_REGION[0],
    lon_e=NH_SOURCE_REGION[2],
):
    from PIL import Image, ImageDraw
    import numpy as np

    with Image.open(out_file) as src:
        src_img = src.convert('RGB')
    sw, sh = src_img.size
    src_px = src_img.load()
    src_arr = np.asarray(src_img, dtype=np.float32)
    row_mean = src_arr.mean(axis=1)
    edge_blend = max(8, min(48, sw // 40))
    if sw > (edge_blend * 2 + 2):
        for yy in range(sh):
            for i in range(edge_blend):
                li = i
                ri = sw - edge_blend + i
                left = src_px[li, yy]
                right = src_px[ri, yy]
                w = (i + 1) / float(edge_blend + 1)
                lmix = (
                    int(round(left[0] * (1.0 - w) + right[0] * w)),
                    int(round(left[1] * (1.0 - w) + right[1] * w)),
                    int(round(left[2] * (1.0 - w) + right[2] * w)),
                )
                rmix = (
                    int(round(right[0] * (1.0 - w) + left[0] * w)),
                    int(round(right[1] * (1.0 - w) + left[1] * w)),
                    int(round(right[2] * (1.0 - w) + left[2] * w)),
                )
                src_px[li, yy] = lmix
                src_px[ri, yy] = rmix

    out_size = NH_POLAR_DIMS
    out_img = Image.new('RGB', (out_size, out_size), color=(214, 214, 214))
    out_px = out_img.load()

    cx = (out_size - 1) / 2.0
    cy = (out_size - 1) / 2.0
    radius = out_size * 0.48
    tan_edge = math.tan((math.pi / 4.0) - (math.radians(lat_min) / 2.0))
    if tan_edge <= 0:
        tan_edge = 1e-6
    lon_span = float(lon_e) - float(lon_w)
    if lon_span <= 0:
        lon_span = 360.0
    pole_cap = 0.018  # blend polar cap using zonal mean to avoid single-longitude pinwheel artifacts.

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

            # Map source longitude using the actual rectangular source bounds.
            lon_rel = lon - float(lon_w)
            while lon_rel < 0.0:
                lon_rel += 360.0
            while lon_rel >= lon_span:
                lon_rel -= lon_span
            sx_f = (lon_rel / lon_span) * (sw - 1)
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

            if r < pole_cap:
                c0 = row_mean[y0]
                c1 = row_mean[y1]
                out_px[x, y] = (
                    int(round(c0[0] * (1.0 - wy) + c1[0] * wy)),
                    int(round(c0[1] * (1.0 - wy) + c1[1] * wy)),
                    int(round(c0[2] * (1.0 - wy) + c1[2] * wy)),
                )
                continue

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


def stitch_horizontal_many(tile_files, out_file):
    from PIL import Image

    images = []
    try:
        for path in tile_files:
            with Image.open(path) as src:
                images.append(src.convert('RGB'))
        if not images:
            raise ValueError('No tile files provided for horizontal stitch.')
        height = min(img.height for img in images)
        resized = [img.resize((img.width, height)) if img.height != height else img for img in images]
        total_width = sum(img.width for img in resized)
        stitched = Image.new('RGB', (total_width, height))
        x = 0
        for img in resized:
            stitched.paste(img, (x, 0))
            x += img.width
        stitched.save(out_file, format='JPEG', quality=97, subsampling=0)
    finally:
        for img in images:
            try:
                img.close()
            except Exception:
                pass


def export_nh_split_composite(composite, out_file, dimensions, scale=None):
    split_dims = split_nh_dimensions(dimensions)
    west_tmp = f'{out_file}.west_tmp.jpg'
    east_tmp = f'{out_file}.east_tmp.jpg'
    try:
        export_composite(
            composite.clip(NH_W),
            west_tmp,
            NH_W_BOUNDS,
            dimensions=split_dims,
            scale=scale,
            crs=TARGET_CRS,
        )
        export_composite(
            composite.clip(NH_E),
            east_tmp,
            NH_E_BOUNDS,
            dimensions=split_dims,
            scale=scale,
            crs=TARGET_CRS,
        )
        stitch_horizontal(west_tmp, east_tmp, out_file)
    finally:
        for tmp in (west_tmp, east_tmp):
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass


def export_composite(composite, out_file, region, dimensions=1600, scale=None, crs=None):
    print(f'[{ts()}] Exporting {out_file}...')
    t0 = time.time()
    current_region = region
    current_dimensions = dimensions
    current_scale = scale
    max_attempts = 4
    backoff_schedule_s = [2, 4, 7]

    for attempt in range(1, max_attempts + 1):
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

            if attempt < max_attempts and is_memory:
                if current_scale is not None:
                    current_scale = int(max(2000, current_scale * 1.45))
                else:
                    current_dimensions = shrink_dimensions(current_dimensions)
                wait_s = backoff_schedule_s[min(attempt - 1, len(backoff_schedule_s) - 1)]
                print(
                    f'[{ts()}] Retry {attempt}/{max_attempts - 1} for {out_file} after memory limit '
                    f'(wait {wait_s}s, next scale={current_scale}, next dims={current_dimensions}).'
                )
                time.sleep(wait_s)
                continue

            if attempt < max_attempts and is_transform:
                new_region = inset_region_bbox(current_region)
                if new_region != current_region:
                    current_region = new_region
                    print(
                        f'[{ts()}] Retry {attempt}/{max_attempts - 1} for {out_file} '
                        f'with inset region to bypass transform edge.'
                    )
                    continue

            raise

    print(f'[{ts()}] Export complete for {out_file} ({time.time() - t0:.2f}s)')


def _generate_z500_anomaly_map_local(img, h, region, prefix):
    if prefix == 'nh_z500a':
        map_dims = NH_SOURCE_DIMS
        sample_bounds = NH_SOURCE_REGION
        plans = [
            {'dims': map_dims, 'sample_scale_m': LOCAL_Z500_NH_SCALES_M[0], 'minor_interval': 6, 'major_interval': 12, 'include_z540': True, 'include_border': True, 'label': 'local base'},
            {'dims': shrink_dimensions(map_dims), 'sample_scale_m': LOCAL_Z500_NH_SCALES_M[1], 'minor_interval': 0, 'major_interval': 18, 'include_z540': True, 'include_border': True, 'label': 'local coarse'},
            {'dims': shrink_dimensions(shrink_dimensions(map_dims)), 'sample_scale_m': LOCAL_Z500_NH_SCALES_M[2], 'minor_interval': 0, 'major_interval': 24, 'include_z540': False, 'include_border': True, 'label': 'local extra coarse'},
            {'dims': shrink_dimensions(shrink_dimensions(map_dims)), 'sample_scale_m': int(LOCAL_Z500_NH_SCALES_M[2] * 1.7), 'minor_interval': 0, 'major_interval': 30, 'include_z540': False, 'include_border': False, 'label': 'local emergency'},
        ]
    else:
        map_dims = region_dimensions(ANOMALY_DIMS, region)
        sample_bounds = region
        plans = [
            {'dims': map_dims, 'sample_scale_m': LOCAL_Z500_NA_SCALES_M[0], 'minor_interval': 6, 'major_interval': 12, 'include_z540': True, 'include_border': True, 'label': 'local base'},
            {'dims': shrink_dimensions(map_dims), 'sample_scale_m': LOCAL_Z500_NA_SCALES_M[1], 'minor_interval': 0, 'major_interval': 18, 'include_z540': True, 'include_border': True, 'label': 'local coarse'},
            {'dims': shrink_dimensions(shrink_dimensions(map_dims)), 'sample_scale_m': LOCAL_Z500_NA_SCALES_M[2], 'minor_interval': 0, 'major_interval': 24, 'include_z540': False, 'include_border': True, 'label': 'local extra coarse'},
            {'dims': shrink_dimensions(shrink_dimensions(map_dims)), 'sample_scale_m': int(LOCAL_Z500_NA_SCALES_M[2] * 1.7), 'minor_interval': 0, 'major_interval': 30, 'include_z540': False, 'include_border': False, 'label': 'local emergency'},
        ]

    if SHORT_RANGE_ACCURACY_HOURS > 0 and int(h) <= SHORT_RANGE_ACCURACY_HOURS:
        for i, plan in enumerate(plans):
            factor = 0.82 if i == 0 else 0.88 if i == 1 else 1.0
            plan['sample_scale_m'] = int(max(45000, plan['sample_scale_m'] * factor))

    if is_long_range_hour(h):
        for plan in plans:
            plan['dims'] = adaptive_dimensions_for_hour(plan['dims'], h, long_factor=0.92, min_w=760, min_h=300)
            plan['sample_scale_m'] = int(plan['sample_scale_m'] * 1.2)
            if plan.get('minor_interval', 0) > 0:
                plan['minor_interval'] = max(12, int(plan['minor_interval']))
            plan['major_interval'] = max(18, int(plan['major_interval']))

    out_file = build_frame_path(prefix, h)
    valid_utc = RUN_INIT_UTC + timedelta(hours=int(h))
    sample_geom = ee.Geometry.Rectangle(sample_bounds, geodesic=False)
    last_msg = ''

    for plan in plans:
        border_png = None
        try:
            width, height = parse_dimensions(plan['dims'])
            forecast_m_img = (
                img.select(WN2_Z500_BAND)
                .divide(9.80665)
                .clip(sample_geom)
                .rename('h500_m')
            )
            forecast_m_arr, used_scale = _sample_rect_array(
                forecast_m_img,
                'h500_m',
                sample_bounds,
                plan['sample_scale_m'],
                context=f'{prefix} forecast_h500_m',
            )
            climo_cache_key = (
                'h500',
                valid_utc.strftime('%Y%m%d%H'),
                tuple(round(float(v), 3) for v in sample_bounds),
                int(max(20000, float(used_scale))),
            )
            cached_climo = LOCAL_CLIMO_ARRAY_CACHE.get(climo_cache_key)
            if cached_climo is None:
                climo_source = _select_climo_source_collection(
                    CLIMO_H500_COLLECTION,
                    valid_utc,
                    sample_geom,
                    'h500',
                )
                climo_m_arr, _ = _sample_grouped_climo_array(
                    climo_source,
                    CLIMO_H500_BAND,
                    sample_bounds,
                    used_scale,
                    group_years=3,
                    context=f'{prefix} climo_h500_m',
                )
                LOCAL_CLIMO_ARRAY_CACHE[climo_cache_key] = climo_m_arr
            else:
                climo_m_arr = cached_climo
            forecast_m_arr, climo_m_arr = _align_arrays(forecast_m_arr, climo_m_arr)
            anomaly_m_arr = forecast_m_arr - climo_m_arr
            contour_dam_arr = forecast_m_arr / 10.0

            _render_local_anomaly_tile(
                anomaly_field=anomaly_m_arr,
                contour_field=contour_dam_arr,
                bounds=sample_bounds,
                out_file=out_file,
                width=width,
                height=height,
                palette=ANOMALY_PALETTE,
                vmin=ANOMALY_MIN_M,
                vmax=ANOMALY_MAX_M,
                minor_interval=plan['minor_interval'],
                major_interval=plan['major_interval'],
                minor_color='#2a2a2a',
                major_color='#141414',
                major_lw=1.0,
                highlight_level=(540 if plan['include_z540'] else None),
                highlight_color='#2455ff',
            )

            if plan.get('include_border', True):
                border_png = get_cached_border_overlay_png(
                    sample_bounds,
                    width,
                    height,
                    include_states=False,
                    detailed=False,
                )
                _overlay_png_on_jpg(out_file, border_png)

            if prefix == 'nh_z500a':
                remap_nh_to_polar(out_file, lon0=NH_LON0)
            annotate_map_file(out_file, prefix, h)
            return
        except Exception as e:
            last_msg = str(e)
            if not _is_memory_or_invalid_argument_error(last_msg):
                # Local path can fail on varying sample dimensions; continue to next plan first.
                if 'sampleRectangle failed' not in last_msg and 'Invalid sampled array shape' not in last_msg:
                    raise
            print(f'[{ts()}] {prefix} hour {h}: local true anomaly retry failed ({plan["label"]}).')

    short_msg = (last_msg or 'Unknown export error').replace('\n', ' ')[:220]
    raise RuntimeError(
        f'{prefix} hour {h}: local true-anomaly export failed after retries. Last error: {short_msg}'
    )


def generate_z500_anomaly_map(img, h, region, prefix):
    if LOCAL_TRUE_ANOMALY_RENDER:
        return _generate_z500_anomaly_map_local(img, h, region, prefix)

    tile_parts = 4
    if prefix == 'nh_z500a':
        map_dims = NH_SOURCE_DIMS
        tile_bounds = split_region_longitude(NH_SOURCE_REGION, parts=tile_parts)
        default_scale = ANOMALY_NH_SCALE_M
    else:
        map_dims = region_dimensions(ANOMALY_DIMS, region)
        tile_bounds = split_region_longitude(region, parts=tile_parts)
        default_scale = ANOMALY_NA_SCALE_M

    def _build_tile_composite(
        tile_geom,
        anomaly_field,
        contour_field,
        anomaly_scale_m,
        contour_scale_m,
        minor_interval=Z500_MINOR_CONTOUR_INTERVAL,
        major_interval=Z500_MAJOR_CONTOUR_INTERVAL,
        include_z540=True,
        include_border=True,
    ):
        anomaly_for_render = _coarsen_for_compute(anomaly_field, anomaly_scale_m, min_scale_m=25000)
        contour_for_render = _coarsen_for_compute(contour_field, contour_scale_m, min_scale_m=25000)
        overlays = [
            flat_background_overlay(tile_geom, color=BASEMAP_OCEAN_COLOR),
            anomaly_overlay(anomaly_for_render),
        ]
        if minor_interval and int(minor_interval) < int(major_interval):
            overlays.append(
                contour_overlay(
                    contour_for_render,
                    interval=minor_interval,
                    color='#202020',
                    opacity=0.38,
                    smooth_px=0,
                    thicken_px=0,
                    line_width_frac=0.006,
                )
            )
        overlays.append(
            contour_overlay(
                contour_for_render,
                interval=major_interval,
                color='#121212',
                opacity=0.92,
                smooth_px=0,
                thicken_px=0,
                line_width_frac=0.009,
            )
        )
        if include_z540:
            overlays.append(
                highlight_iso_overlay(
                    contour_for_render,
                    level=540,
                    color='#2455ff',
                    opacity=0.92,
                    tolerance=1.0,
                    smooth_px=0,
                )
            )
        if include_border:
            overlays.append(
                border_overlay(
                    include_states=False,
                    region_geom=tile_geom,
                    detailed=False,
                )
            )
        return ee.ImageCollection(overlays).mosaic()

    def _is_recoverable_export_error(msg):
        return (
            'User memory limit exceeded' in msg
            or 'Unable to transform edge' in msg
            or 'Invalid argument' in msg
            or 'INVALID_ARGUMENT' in msg
        )

    if prefix == 'nh_z500a':
        mid_dims = shrink_dimensions(map_dims)
        low_dims = shrink_dimensions(mid_dims)
        contour_scale_base = int(ANOMALY_WORK_SCALE_M)
        plans = [
            {
                'use_scale': True,
                'dims': mid_dims,
                'scale_m': int(ANOMALY_NH_SCALE_M * 1.4),
                'anomaly_scale_m': Z500_NH_ANOM_SCALES_M[0],
                'contour_scale_m': int(contour_scale_base * 1.2),
                'minor_interval': Z500_MAJOR_CONTOUR_INTERVAL,
                'major_interval': Z500_MAJOR_CONTOUR_INTERVAL,
                'label': 'split scale + major contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'scale_m': int(ANOMALY_NH_SCALE_M * 2.0),
                'anomaly_scale_m': Z500_NH_ANOM_SCALES_M[1],
                'contour_scale_m': int(contour_scale_base * 1.6),
                'minor_interval': 18,
                'major_interval': 18,
                'label': 'split coarse scale + sparse contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'scale_m': int(ANOMALY_NH_SCALE_M * 2.6),
                'anomaly_scale_m': Z500_NH_ANOM_SCALES_M[2],
                'contour_scale_m': int(contour_scale_base * 2.0),
                'minor_interval': 24,
                'major_interval': 24,
                'label': 'split ultra coarse scale + ultra sparse contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'scale_m': int(ANOMALY_NH_SCALE_M * 4.2),
                'anomaly_scale_m': int(Z500_NH_ANOM_SCALES_M[2] * 1.5),
                'contour_scale_m': int(contour_scale_base * 2.8),
                'minor_interval': 0,
                'major_interval': 30,
                'include_z540': False,
                'include_border': False,
                'label': 'emergency ultra coarse true anomaly',
            },
        ]
    else:
        mid_dims = shrink_dimensions(map_dims)
        low_dims = shrink_dimensions(mid_dims)
        contour_scale_base = int(ANOMALY_WORK_SCALE_M)
        plans = [
            {
                'use_scale': True,
                'dims': mid_dims,
                'anomaly_scale_m': Z500_NA_ANOM_SCALES_M[0],
                'scale_m': int(ANOMALY_NA_SCALE_M * 1.6),
                'contour_scale_m': int(contour_scale_base * 1.2),
                'minor_interval': Z500_MAJOR_CONTOUR_INTERVAL,
                'major_interval': Z500_MAJOR_CONTOUR_INTERVAL,
                'label': 'scale + major contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'anomaly_scale_m': Z500_NA_ANOM_SCALES_M[1],
                'scale_m': int(ANOMALY_NA_SCALE_M * 2.2),
                'contour_scale_m': int(contour_scale_base * 1.6),
                'minor_interval': 18,
                'major_interval': 18,
                'label': 'coarse scale + sparse contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'scale_m': int(ANOMALY_NA_SCALE_M * 2.9),
                'anomaly_scale_m': Z500_NA_ANOM_SCALES_M[2],
                'contour_scale_m': int(contour_scale_base * 2.0),
                'minor_interval': 24,
                'major_interval': 24,
                'label': 'ultra coarse scale + ultra sparse contours',
            },
            {
                'use_scale': True,
                'dims': low_dims,
                'scale_m': int(ANOMALY_NA_SCALE_M * 4.5),
                'anomaly_scale_m': int(Z500_NA_ANOM_SCALES_M[2] * 1.5),
                'contour_scale_m': int(contour_scale_base * 2.8),
                'minor_interval': 0,
                'major_interval': 30,
                'include_z540': False,
                'include_border': False,
                'label': 'emergency ultra coarse true anomaly',
            },
        ]

    if is_long_range_hour(h):
        for plan in plans:
            plan['dims'] = adaptive_dimensions_for_hour(plan['dims'], h, long_factor=0.92, min_w=700, min_h=320)
            if plan.get('scale_m') is not None:
                plan['scale_m'] = int(plan['scale_m'] * 1.15)
            if plan.get('anomaly_scale_m') is not None:
                plan['anomaly_scale_m'] = int(plan['anomaly_scale_m'] * 1.2)
            if plan.get('contour_scale_m') is not None:
                plan['contour_scale_m'] = int(plan['contour_scale_m'] * 1.2)
            plan['major_interval'] = max(18, int(plan['major_interval']))
            if plan.get('minor_interval', 0) > 0:
                plan['minor_interval'] = max(12, int(plan['minor_interval']))

    out_file = build_frame_path(prefix, h)
    last_msg = ''
    for plan in plans:
        tile_tmp_files = [f'{out_file}.tile{idx}.jpg' for idx in range(len(tile_bounds))]
        export_scale = (plan.get('scale_m', default_scale) if plan.get('use_scale', True) else None)
        split_dims = split_dimensions_horizontal_parts(plan.get('dims') or map_dims, parts=len(tile_bounds))
        try:
            for idx, bounds in enumerate(tile_bounds):
                tile_geom = ee.Geometry.Rectangle(bounds, geodesic=False)
                contour_tile = img.select(WN2_Z500_BAND).divide(9.80665).divide(10).clip(tile_geom)
                anomaly_tile = z500_anomaly_m(
                    img,
                    h,
                    region_geom=tile_geom,
                    cache_tag=f'{prefix}_{plan["anomaly_scale_m"]}_{idx}',
                    analysis_scale_m=plan['anomaly_scale_m'],
                )
                tile_composite = _build_tile_composite(
                    tile_geom=tile_geom,
                    anomaly_field=anomaly_tile,
                    contour_field=contour_tile,
                    anomaly_scale_m=plan['anomaly_scale_m'],
                    contour_scale_m=plan['contour_scale_m'],
                    minor_interval=plan['minor_interval'],
                    major_interval=plan['major_interval'],
                    include_z540=plan.get('include_z540', True),
                    include_border=plan.get('include_border', True),
                )
                export_composite(
                    tile_composite,
                    tile_tmp_files[idx],
                    bounds,
                    dimensions=split_dims,
                    scale=export_scale,
                    crs=TARGET_CRS,
                )

            stitch_horizontal_many(tile_tmp_files, out_file)
            if prefix == 'nh_z500a':
                remap_nh_to_polar(out_file, lon0=NH_LON0)
            annotate_map_file(out_file, prefix, h)
            return
        except Exception as e:
            msg = str(e)
            if not _is_recoverable_export_error(msg):
                raise
            last_msg = msg
            print(f'[{ts()}] {prefix} hour {h}: true anomaly export retry failed ({plan["label"]}).')
        finally:
            for tmp in tile_tmp_files:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass

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


def frame_exists_valid(path):
    p = Path(path)
    if not p.exists():
        return False
    try:
        return p.stat().st_size >= MIN_VALID_FRAME_BYTES
    except OSError:
        return False


def should_render_frame(path):
    if not RESUME_EXISTING:
        return True
    return not frame_exists_valid(path)


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
    mslp_interval = adaptive_interval_for_hour(3, h, long_interval=4)
    mslp_contours = contour_overlay(
        mslp_hpa,
        interval=mslp_interval,
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
    dims = adaptive_dimensions_for_hour(region_dimensions(base_dims, region), h, long_factor=0.92, min_w=980, min_h=720)
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
    dims = adaptive_dimensions_for_hour(region_dimensions(base_dims, region), h, long_factor=0.92, min_w=980, min_h=720)
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h, map_region=region, snow_labels=snow_labels, snow_ratio=snow_ratio)


def generate_conus_t2m_map(img, h, region=CONUS_THUMB_REGION, key='conus_t2m'):
    region_geom = ee.Geometry.Rectangle(region, geodesic=False)
    t2m_f = (
        img.select(WN2_T2M_BAND)
        .subtract(273.15)
        .multiply(9.0 / 5.0)
        .add(32.0)
        .clip(region_geom)
    )
    t2m_vis = t2m_f.resample('bilinear').focalMean(1, 'circle', 'pixels')
    t2m_layer = t2m_vis.visualize(min=T2M_F_MIN, max=T2M_F_MAX, palette=T2M_F_PALETTE)
    temp_interval = adaptive_interval_for_hour(8, h, long_interval=10)
    t2m_contours = contour_overlay(
        t2m_vis,
        interval=temp_interval,
        color='#2a2a2a',
        opacity=0.62,
        smooth_px=0,
        thicken_px=0,
    )
    freezing_line = highlight_iso_overlay(
        t2m_vis,
        level=32,
        color='#2455ff',
        opacity=0.88,
        tolerance=0.9,
        smooth_px=0,
    )
    composite = ee.ImageCollection([
        basemap_overlay(region_geom, land_color=BASEMAP_LAND_COLOR, ocean_color=BASEMAP_OCEAN_COLOR),
        t2m_layer,
        t2m_contours,
        freezing_line,
        border_overlay(include_states=True),
    ]).mosaic()
    out_file = build_frame_path(key, h)
    dims = adaptive_dimensions_for_hour(region_dimensions(CONUS_DIMS, region), h, long_factor=0.92, min_w=980, min_h=720)
    export_composite(composite, out_file, region, dimensions=dims)
    annotate_map_file(out_file, key, h)


def _generate_conus_t2m_anomaly_map_local(img, h, region=CONUS_THUMB_REGION, key='conus_t2m_anom'):
    base_dims = region_dimensions(CONUS_DIMS, region)
    plans = [
        {'dims': base_dims, 'sample_scale_m': LOCAL_T2M_ANOM_SCALES_M[0], 'contour_interval': 12, 'include_freezing': True, 'include_border': True, 'label': 'local base'},
        {'dims': shrink_dimensions(base_dims), 'sample_scale_m': LOCAL_T2M_ANOM_SCALES_M[1], 'contour_interval': 18, 'include_freezing': True, 'include_border': True, 'label': 'local coarse'},
        {'dims': shrink_dimensions(shrink_dimensions(base_dims)), 'sample_scale_m': LOCAL_T2M_ANOM_SCALES_M[2], 'contour_interval': 24, 'include_freezing': False, 'include_border': True, 'label': 'local extra coarse'},
        {'dims': shrink_dimensions(shrink_dimensions(base_dims)), 'sample_scale_m': int(LOCAL_T2M_ANOM_SCALES_M[2] * 1.8), 'contour_interval': 30, 'include_freezing': False, 'include_border': False, 'label': 'local emergency'},
    ]
    if is_long_range_hour(h):
        for plan in plans:
            plan['dims'] = adaptive_dimensions_for_hour(plan['dims'], h, long_factor=0.92, min_w=760, min_h=520)
            plan['sample_scale_m'] = int(plan['sample_scale_m'] * 1.2)
            plan['contour_interval'] = max(18, int(plan['contour_interval']))

    out_file = build_frame_path(key, h)
    valid_utc = RUN_INIT_UTC + timedelta(hours=int(h))
    sample_geom = ee.Geometry.Rectangle(region, geodesic=False)
    last_msg = ''

    for plan in plans:
        border_png = None
        try:
            width, height = parse_dimensions(plan['dims'])
            forecast_c_img = (
                img.select(WN2_T2M_BAND)
                .subtract(273.15)
                .clip(sample_geom)
                .rename('t2m_c')
            )
            forecast_c_arr, used_scale = _sample_rect_array(
                forecast_c_img,
                't2m_c',
                region,
                plan['sample_scale_m'],
                context=f'{key} forecast_t2m_c',
            )
            climo_cache_key = (
                't2m',
                valid_utc.strftime('%Y%m%d%H'),
                tuple(round(float(v), 3) for v in region),
                int(max(20000, float(used_scale))),
            )
            cached_climo = LOCAL_CLIMO_ARRAY_CACHE.get(climo_cache_key)
            if cached_climo is None:
                climo_source = _select_climo_source_collection(
                    CLIMO_T2M_COLLECTION,
                    valid_utc,
                    sample_geom,
                    't2m',
                )
                climo_k_arr, _ = _sample_grouped_climo_array(
                    climo_source,
                    'T2M',
                    region,
                    used_scale,
                    group_years=3,
                    context=f'{key} climo_t2m_c',
                )
                climo_c_arr = climo_k_arr - 273.15
                LOCAL_CLIMO_ARRAY_CACHE[climo_cache_key] = climo_c_arr
            else:
                climo_c_arr = cached_climo
            forecast_c_arr, climo_c_arr = _align_arrays(forecast_c_arr, climo_c_arr)
            anomaly_f_arr = (forecast_c_arr - climo_c_arr) * (9.0 / 5.0)
            contour_f_arr = forecast_c_arr * (9.0 / 5.0) + 32.0

            _render_local_anomaly_tile(
                anomaly_field=anomaly_f_arr,
                contour_field=contour_f_arr,
                bounds=region,
                out_file=out_file,
                width=width,
                height=height,
                palette=T2M_ANOM_F_PALETTE,
                vmin=T2M_ANOM_F_MIN,
                vmax=T2M_ANOM_F_MAX,
                minor_interval=None,
                major_interval=int(plan['contour_interval']),
                minor_color='#2d2d2d',
                major_color='#222222',
                major_lw=1.0,
                highlight_level=(32 if plan.get('include_freezing', True) else None),
                highlight_color='#2455ff',
            )

            if plan.get('include_border', True):
                border_png = get_cached_border_overlay_png(
                    region,
                    width,
                    height,
                    include_states=True,
                    detailed=False,
                )
                _overlay_png_on_jpg(out_file, border_png)

            annotate_map_file(out_file, key, h)
            return
        except Exception as e:
            last_msg = str(e)
            if not _is_memory_or_invalid_argument_error(last_msg):
                if 'sampleRectangle failed' not in last_msg and 'Invalid sampled array shape' not in last_msg:
                    raise
            print(f'[{ts()}] {key} hour {h}: local true anomaly retry failed ({plan["label"]}).')

    short_msg = (last_msg or 'Unknown export error').replace('\n', ' ')[:220]
    raise RuntimeError(
        f'{key} hour {h}: local true-anomaly export failed after retries. Last error: {short_msg}'
    )


def generate_conus_t2m_anomaly_map(img, h, region=CONUS_THUMB_REGION, key='conus_t2m_anom'):
    if LOCAL_TRUE_ANOMALY_RENDER:
        return _generate_conus_t2m_anomaly_map_local(img, h, region=region, key=key)

    tile_bounds = split_region_longitude(region, parts=4)
    base_dims = region_dimensions(CONUS_DIMS, region)
    mid_dims = shrink_dimensions(base_dims)
    low_dims = shrink_dimensions(mid_dims)
    plans = [
        {'dims': mid_dims, 'scale_m': 30000, 'work_scale_m': T2M_ANOM_WORK_SCALES_M[0], 'contour_interval': 12, 'label': 'scaled export + 12F contours'},
        {'dims': low_dims, 'scale_m': 42000, 'work_scale_m': T2M_ANOM_WORK_SCALES_M[1], 'contour_interval': 18, 'label': 'coarse scale + sparse contours'},
        {'dims': low_dims, 'scale_m': 56000, 'work_scale_m': T2M_ANOM_WORK_SCALES_M[2], 'contour_interval': 24, 'label': 'ultra coarse scale + ultra sparse contours'},
        {'dims': low_dims, 'scale_m': 90000, 'work_scale_m': int(T2M_ANOM_WORK_SCALES_M[2] * 1.6), 'contour_interval': 30, 'include_border': False, 'include_freezing': False, 'label': 'emergency ultra coarse true anomaly'},
    ]
    if is_long_range_hour(h):
        for plan in plans:
            plan['dims'] = adaptive_dimensions_for_hour(plan['dims'], h, long_factor=0.92, min_w=760, min_h=520)
            plan['scale_m'] = int(plan['scale_m'] * 1.15)
            plan['work_scale_m'] = int(plan['work_scale_m'] * 1.2)
            plan['contour_interval'] = max(18, int(plan['contour_interval']))

    def _build_composite(
        tile_geom,
        t2m_anom_f,
        t2m_f,
        work_scale_m,
        contour_interval,
        include_border=True,
        include_freezing=True,
    ):
        anom_field = _coarsen_for_compute(t2m_anom_f, work_scale_m, min_scale_m=15000)
        contour_field = _coarsen_for_compute(t2m_f, max(work_scale_m, 28000), min_scale_m=20000)
        t2m_anom_vis = anom_field.resample('bilinear').focalMean(1, 'circle', 'pixels')
        t2m_anom_layer = t2m_anom_vis.visualize(
            min=T2M_ANOM_F_MIN,
            max=T2M_ANOM_F_MAX,
            palette=T2M_ANOM_F_PALETTE,
        )
        temp_contours = contour_overlay(
            contour_field,
            interval=int(contour_interval),
            color='#2d2d2d',
            opacity=0.50,
            smooth_px=0,
            thicken_px=0,
        )
        overlays = [
            flat_background_overlay(tile_geom, color=BASEMAP_OCEAN_COLOR),
            t2m_anom_layer,
            temp_contours,
        ]
        if include_freezing:
            overlays.append(
                highlight_iso_overlay(
                    contour_field,
                    level=32,
                    color='#2455ff',
                    opacity=0.90,
                    tolerance=1.0,
                    smooth_px=0,
                )
            )
        if include_border:
            overlays.append(border_overlay(include_states=True, region_geom=tile_geom))
        return ee.ImageCollection(overlays).mosaic()

    def _is_recoverable_export_error(msg):
        return (
            'User memory limit exceeded' in msg
            or 'Unable to transform edge' in msg
            or 'Invalid argument' in msg
            or 'INVALID_ARGUMENT' in msg
        )

    out_file = build_frame_path(key, h)
    last_msg = ''
    for plan in plans:
        tile_tmp_files = [f'{out_file}.tile{idx}.jpg' for idx in range(len(tile_bounds))]
        split_dims = split_dimensions_horizontal_parts(plan['dims'], parts=len(tile_bounds))
        try:
            for idx, bounds in enumerate(tile_bounds):
                tile_geom = ee.Geometry.Rectangle(bounds, geodesic=False)
                t2m_f = (
                    img.select(WN2_T2M_BAND)
                    .subtract(273.15)
                    .multiply(9.0 / 5.0)
                    .add(32.0)
                    .clip(tile_geom)
                )
                t2m_anom_c = t2m_anomaly_c(
                    img,
                    h,
                    region_geom=tile_geom,
                    cache_tag=f'{key}_{plan["work_scale_m"]}_{idx}',
                    analysis_scale_m=plan['work_scale_m'],
                )
                t2m_anom_f = t2m_anom_c.multiply(9.0 / 5.0).rename('t2m_anomaly_f')
                composite = _build_composite(
                    tile_geom=tile_geom,
                    t2m_anom_f=t2m_anom_f,
                    t2m_f=t2m_f,
                    work_scale_m=plan['work_scale_m'],
                    contour_interval=plan['contour_interval'],
                    include_border=plan.get('include_border', True),
                    include_freezing=plan.get('include_freezing', True),
                )
                export_composite(
                    composite,
                    tile_tmp_files[idx],
                    bounds,
                    dimensions=split_dims,
                    scale=plan['scale_m'],
                    crs=TARGET_CRS,
                )

            stitch_horizontal_many(tile_tmp_files, out_file)
            annotate_map_file(out_file, key, h)
            return
        except Exception as e:
            msg = str(e)
            if not _is_recoverable_export_error(msg):
                raise
            last_msg = msg
            print(f'[{ts()}] {key} hour {h}: anomaly export retry failed ({plan["label"]}).')
        finally:
            for tmp in tile_tmp_files:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass

    short_msg = (last_msg or 'Unknown export error').replace('\n', ' ')[:220]
    raise RuntimeError(f'{key} hour {h}: true-anomaly export failed after retries. Last error: {short_msg}')


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
    z500_interval = adaptive_interval_for_hour(6, h, long_interval=8)
    z500_contours = contour_overlay(
        z500_height_dam,
        interval=z500_interval,
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
    dims = adaptive_dimensions_for_hour(
        region_dimensions(CONUS_DIMS, CONUS_THUMB_REGION),
        h,
        long_factor=0.92,
        min_w=980,
        min_h=720,
    )
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
if SKIP_CLEANUP_RUN_DIR:
    print(f'[{ts()}] Skipping run-directory cleanup to preserve existing frames for resume mode.')
else:
    cleanup_current_run_products()
failures = []
successful_exports = 0
planned_exports = 0
skipped_existing_exports = 0
failed_product_keys = set()
failed_product_messages = {}
needs_snow_accum = any(k in SNOW_PRODUCT_KEYS for k, _, _ in ENABLED_PRODUCTS)
snow_accum_by_hour = {}
zero_snow = ee.Image.constant(0).clip(CONUS_REGION).rename('snow_total_cm')
snow_precompute_done = False

enabled_keys = {k for k, _, _ in ENABLED_PRODUCTS}
hour_image_cache = {}


def get_cached_hour_image(hour):
    cached = hour_image_cache.get(hour)
    if cached is None:
        cached = get_hour_image(hour)
        hour_image_cache[hour] = cached
    return cached


def _record_task_failure(hour, name, err_msg, exc):
    print(f'[{ts()}] Hour {hour} product {name}: FAILED - {err_msg}')
    failures.append((f'{hour}:{name}', err_msg))
    product_key = None
    enabled_product_keys = [key for key, _, _ in ENABLED_PRODUCTS]
    if name in enabled_product_keys:
        product_key = name
    else:
        prefix_matches = [key for key in enabled_product_keys if name.startswith(f'{key}_')]
        if prefix_matches:
            product_key = max(prefix_matches, key=len)
    if product_key is None:
        product_key = name
    failed_product_keys.add(product_key)
    msg_text = str(err_msg).replace('\n', ' ').strip()
    if len(msg_text) > 500:
        msg_text = msg_text[:500] + '...'
    failed_product_messages[product_key] = msg_text
    if 'earthengine.thumbnails.create' in err_msg:
        raise RuntimeError(
            "Earth Engine permission denied: earthengine.thumbnails.create. "
            "Grant the service account Earth Engine User (or Admin) on EE_PROJECT and ensure Earth Engine API is enabled."
        ) from exc


def ensure_snow_accum_precompute():
    global snow_precompute_done, snow_accum_by_hour

    if snow_precompute_done or not needs_snow_accum:
        return
    selected_hours = sorted(set(HOURS))
    if not selected_hours:
        snow_precompute_done = True
        return

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

    local_map = {}
    for sh in selected_hours:
        eligible = [hh for hh in running_by_available_hour.keys() if hh <= sh]
        if eligible:
            local_map[sh] = running_by_available_hour[max(eligible)]
        else:
            local_map[sh] = zero_snow
    snow_accum_by_hour = local_map
    snow_precompute_done = True
    print(
        f'[{ts()}] Snow accumulation precompute: '
        f'available_steps={len(accum_hours)}, selected_frames={len(selected_hours)}, '
        f'max_hour={max_selected_hour}.'
    )


def write_step_summary(
    success_count,
    failure_items,
    enabled_products=None,
    generated_products=None,
    missing_products=None,
    planned_count=0,
    skipped_existing_count=0,
):
    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if not summary_path:
        return
    try:
        enabled_products = list(enabled_products or [])
        generated_products = list(generated_products or [])
        missing_products = list(missing_products or [])
        lines = [
            '## WeatherNext2 Render Summary',
            f'- Successful exports: {int(success_count)}',
            f'- Failed exports: {len(failure_items)}',
        ]
        if planned_count:
            lines.append(f'- Planned exports this run: {int(planned_count)}')
        if skipped_existing_count:
            lines.append(f'- Skipped existing frames: {int(skipped_existing_count)}')
        if enabled_products:
            lines.append(f'- Enabled products: {", ".join(enabled_products)}')
        if generated_products:
            lines.append(f'- Products with frames generated: {", ".join(generated_products)}')
        if missing_products:
            lines.append(f'- Missing products (no frames): {", ".join(missing_products)}')
        if failure_items:
            lines.append('')
            lines.append('### Failures')
            for hour_key, msg in failure_items[:20]:
                one_line = str(msg).replace('\n', ' ').strip()
                if len(one_line) > 180:
                    one_line = one_line[:180] + '...'
                lines.append(f'- `{hour_key}`: {one_line}')
            if len(failure_items) > 20:
                lines.append(f'- ... and {len(failure_items) - 20} more')
        lines.append('')
        with open(summary_path, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
    except Exception as e:
        print(f'[{ts()}] Warning: could not write GitHub step summary: {e}')


if 'nh_z500a' in enabled_keys:
    print(f'[{ts()}] Phase 1/3: generating NH z500 true-anomaly maps first.')
    for h in HOURS:
        out_file = build_frame_path('nh_z500a', h)
        if not should_render_frame(out_file):
            skipped_existing_exports += 1
            continue
        planned_exports += 1
        print(f'Generating Hour {h} [NH z500]...')
        img = get_cached_hour_image(h)
        try:
            generate_z500_anomaly_map(img, h, NH_THUMB_REGION, 'nh_z500a')
            successful_exports += 1
        except Exception as e:
            _record_task_failure(h, 'nh_z500a', str(e), e)

non_z500_enabled = [k for k in enabled_keys if k not in {'nh_z500a', 'na_z500a'}]
if non_z500_enabled:
    print(f'[{ts()}] Phase 2/3: generating non-z500 products.')
    for h in HOURS:
        task_specs = []

        if 'conus_mslp_ptype' in enabled_keys:
            out_file = build_frame_path('conus_mslp_ptype', h)
            if should_render_frame(out_file):
                task_specs.append(('conus_mslp_ptype', 'conus_mslp_ptype', None))
            else:
                skipped_existing_exports += 1
        if 'ne_mslp_ptype' in enabled_keys:
            out_file = build_frame_path('ne_mslp_ptype', h)
            if should_render_frame(out_file):
                task_specs.append(('ne_mslp_ptype', 'ne_mslp_ptype', None))
            else:
                skipped_existing_exports += 1
        if 'conus_vort500' in enabled_keys:
            out_file = build_frame_path('conus_vort500', h)
            if should_render_frame(out_file):
                task_specs.append(('conus_vort500', 'conus_vort500', None))
            else:
                skipped_existing_exports += 1
        if 'conus_t2m' in enabled_keys:
            out_file = build_frame_path('conus_t2m', h)
            if should_render_frame(out_file):
                task_specs.append(('conus_t2m', 'conus_t2m', None))
            else:
                skipped_existing_exports += 1
        if 'conus_t2m_anom' in enabled_keys:
            out_file = build_frame_path('conus_t2m_anom', h)
            if should_render_frame(out_file):
                task_specs.append(('conus_t2m_anom', 'conus_t2m_anom', None))
            else:
                skipped_existing_exports += 1
        if 'conus_snow_accum' in enabled_keys:
            for ratio in SNOW_RATIOS:
                out_file = build_frame_path('conus_snow_accum', h, snow_ratio=ratio)
                if should_render_frame(out_file):
                    task_specs.append((f'conus_snow_accum_r{ratio:02d}', 'conus_snow_accum', ratio))
                else:
                    skipped_existing_exports += 1
        if 'ne_snow_accum' in enabled_keys:
            for ratio in SNOW_RATIOS:
                out_file = build_frame_path('ne_snow_accum', h, snow_ratio=ratio)
                if should_render_frame(out_file):
                    task_specs.append((f'ne_snow_accum_r{ratio:02d}', 'ne_snow_accum', ratio))
                else:
                    skipped_existing_exports += 1
        if 'ne_zoom_snow_accum' in enabled_keys:
            for ratio in SNOW_RATIOS:
                out_file = build_frame_path('ne_zoom_snow_accum', h, snow_ratio=ratio)
                if should_render_frame(out_file):
                    task_specs.append((f'ne_zoom_snow_accum_r{ratio:02d}', 'ne_zoom_snow_accum', ratio))
                else:
                    skipped_existing_exports += 1

        if not task_specs:
            continue

        planned_exports += len(task_specs)
        print(f'Generating Hour {h} [core products] ({len(task_specs)} pending export(s))...')
        img = get_cached_hour_image(h)
        needs_snow_here = any(spec[1] in SNOW_PRODUCT_KEYS for spec in task_specs)
        if needs_snow_here:
            ensure_snow_accum_precompute()
            snow_for_hour = snow_accum_by_hour.get(h, zero_snow)
        else:
            snow_for_hour = zero_snow

        tasks = []
        for name, kind, ratio in task_specs:
            if kind == 'conus_mslp_ptype':
                tasks.append((name, lambda i=img, hh=h: generate_mslp_ptype_map(i, hh, CONUS_THUMB_REGION, 'conus_mslp_ptype')))
            elif kind == 'ne_mslp_ptype':
                tasks.append((name, lambda i=img, hh=h: generate_mslp_ptype_map(i, hh, NE_THUMB_REGION, 'ne_mslp_ptype')))
            elif kind == 'conus_vort500':
                tasks.append((name, lambda i=img, hh=h: generate_vort500_map(i, hh)))
            elif kind == 'conus_t2m':
                tasks.append((name, lambda i=img, hh=h: generate_conus_t2m_map(i, hh, CONUS_THUMB_REGION, 'conus_t2m')))
            elif kind == 'conus_t2m_anom':
                tasks.append((name, lambda i=img, hh=h: generate_conus_t2m_anomaly_map(i, hh, CONUS_THUMB_REGION, 'conus_t2m_anom')))
            elif kind == 'conus_snow_accum':
                rr = int(ratio)
                tasks.append((
                    name,
                    lambda i=img, hh=h, s=snow_for_hour, r=rr: generate_snow_accum_map(
                        i,
                        hh,
                        s,
                        CONUS_THUMB_REGION,
                        'conus_snow_accum',
                        snow_ratio=r,
                    ),
                ))
            elif kind == 'ne_snow_accum':
                rr = int(ratio)
                tasks.append((
                    name,
                    lambda i=img, hh=h, s=snow_for_hour, r=rr: generate_snow_accum_map(
                        i,
                        hh,
                        s,
                        NE_THUMB_REGION,
                        'ne_snow_accum',
                        snow_ratio=r,
                    ),
                ))
            elif kind == 'ne_zoom_snow_accum':
                rr = int(ratio)
                tasks.append((
                    name,
                    lambda i=img, hh=h, s=snow_for_hour, r=rr: generate_snow_accum_map(
                        i,
                        hh,
                        s,
                        NE_ZOOM_SNOW_THUMB_REGION,
                        'ne_zoom_snow_accum',
                        snow_ratio=r,
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
                    _record_task_failure(h, name, err_msg, e)

if 'na_z500a' in enabled_keys:
    print(f'[{ts()}] Phase 3/3: generating NA z500 true-anomaly maps last.')
    for h in HOURS:
        out_file = build_frame_path('na_z500a', h)
        if not should_render_frame(out_file):
            skipped_existing_exports += 1
            continue
        planned_exports += 1
        print(f'Generating Hour {h} [NA z500]...')
        img = get_cached_hour_image(h)
        try:
            generate_z500_anomaly_map(img, h, NA_THUMB_REGION, 'na_z500a')
            successful_exports += 1
        except Exception as e:
            _record_task_failure(h, 'na_z500a', str(e), e)

print(
    f'[{ts()}] Export counts: planned={planned_exports}, '
    f'generated={successful_exports}, skipped_existing={skipped_existing_exports}, failed={len(failures)}.'
)

if failures:
    print(f'[{ts()}] Completed with {len(failures)} failed hour(s).')
    for h, msg in failures:
        print(f'[{ts()}] Failure summary - hour {h}: {msg}')

generated_product_keys = []
skip_sanity_checks = RESUME_EXISTING and planned_exports == 0 and successful_exports == 0 and not failures
for key, _, pattern in ENABLED_PRODUCTS:
    product_files = sorted(Path(RUN_OUTPUT_DIR).glob(pattern))
    if not product_files:
        if key in failed_product_keys:
            print(f'[{ts()}] Skipping sanity check for {key}: no frames generated after render failures.')
        else:
            print(f'[{ts()}] Skipping sanity check for {key}: no frames generated this run.')
        continue
    if skip_sanity_checks:
        generated_product_keys.append(key)
        continue
    sanity_check_jpgs(
        str(RUN_OUTPUT_DIR),
        pattern=pattern,
        require_variation=not is_snow_product(key),
    )
    generated_product_keys.append(key)
if skip_sanity_checks:
    print(f'[{ts()}] Sanity checks skipped: resume-only run with no new exports planned.')

enabled_product_keys = [key for key, _, _ in ENABLED_PRODUCTS]
missing_product_keys = [key for key in enabled_product_keys if key not in generated_product_keys]
if missing_product_keys:
    print(
        f'[{ts()}] Product coverage warning: missing frames for {missing_product_keys}; '
        f'generated={generated_product_keys}'
    )

write_step_summary(
    successful_exports,
    failures,
    enabled_products=enabled_product_keys,
    generated_products=generated_product_keys,
    missing_products=missing_product_keys,
    planned_count=planned_exports,
    skipped_existing_count=skipped_existing_exports,
)


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


FRAME_NAME_RE = re.compile(r'^(?P<product>[a-z0-9_]+?)(?:_r(?P<ratio>\d{2}))?_(?P<hour>\d{3})\.jpg$', re.IGNORECASE)


def discover_run_product_frames(run_dir):
    product_hours = {}
    product_snow_ratios = {}
    if not run_dir.exists() or not run_dir.is_dir():
        return {}, {}

    for frame_path in run_dir.glob('*.jpg'):
        match = FRAME_NAME_RE.match(frame_path.name)
        if not match:
            continue
        product = str(match.group('product') or '').strip()
        if not product:
            continue
        hour = _parse_int(match.group('hour'))
        if hour is None:
            continue
        product_hours.setdefault(product, set()).add(hour)
        ratio_text = match.group('ratio')
        if ratio_text is not None:
            ratio = _parse_int(ratio_text)
            if ratio is not None:
                product_snow_ratios.setdefault(product, set()).add(ratio)

    normalized_hours = {
        key: sorted(values)
        for key, values in product_hours.items()
        if values
    }
    normalized_ratios = {
        key: sorted(values)
        for key, values in product_snow_ratios.items()
        if values
    }
    return normalized_hours, normalized_ratios


def normalize_product_hours_map(raw_map):
    out = {}
    if not isinstance(raw_map, dict):
        return out
    for raw_key, raw_values in raw_map.items():
        key = str(raw_key).strip()
        if not key:
            continue
        hours = normalize_int_list(raw_values, min_value=0)
        if hours:
            out[key] = hours
    return out


def normalize_product_ratio_map(raw_map):
    out = {}
    if not isinstance(raw_map, dict):
        return out
    for raw_key, raw_values in raw_map.items():
        key = str(raw_key).strip()
        if not key:
            continue
        ratios = normalize_int_list(raw_values, min_value=10, max_value=20)
        if ratios:
            out[key] = ratios
    return out


def estimate_run_frame_count(entry):
    products = [str(p) for p in entry.get('products', []) if isinstance(p, str)]
    if not products:
        return 0
    product_hours = normalize_product_hours_map(entry.get('product_hours'))
    product_ratios = normalize_product_ratio_map(entry.get('product_snow_ratios'))
    fallback_hours = normalize_int_list(entry.get('hours', []), min_value=0)
    fallback_ratios = normalize_int_list(entry.get('snow_ratios', []), min_value=10, max_value=20)

    frame_count = 0
    for key in products:
        hours = product_hours.get(key, fallback_hours)
        if not hours:
            continue
        if is_snow_product(key):
            ratios = product_ratios.get(key, fallback_ratios)
            if not ratios:
                ratios = [10]
            frame_count += len(hours) * len(ratios)
        else:
            frame_count += len(hours)
    return frame_count


def choose_default_run_id(manifest_runs, fallback_id):
    if not manifest_runs:
        return fallback_id
    total_known_products = len(PRODUCT_OPTIONS)

    def _score(item):
        products = [str(p) for p in item.get('products', []) if isinstance(p, str)]
        product_count = len(products)
        product_coverage = (product_count / float(total_known_products)) if total_known_products > 0 else 0.0
        frame_count = estimate_run_frame_count(item)
        init_dt = parse_iso_utc(item.get('init_utc')) or datetime.min.replace(tzinfo=timezone.utc)
        # Prefer broad product coverage first, then total frame inventory, then recency.
        return (product_coverage, product_count, frame_count, init_dt)

    best = max(manifest_runs, key=_score)
    best_id = str(best.get('id') or '').strip()
    return best_id or fallback_id


manifest_path = Path(OUTPUT) / 'runs_manifest.json'
existing_entries = load_manifest_entries(manifest_path)
same_run_existing_count = sum(1 for item in existing_entries if str(item.get('id', '')).strip() == RUN_ID)
if same_run_existing_count:
    existing_entries = [item for item in existing_entries if str(item.get('id', '')).strip() != RUN_ID]
    print(
        f'[{ts()}] Ignoring {same_run_existing_count} existing manifest entr'
        f'{"y" if same_run_existing_count == 1 else "ies"} for current RUN_ID {RUN_ID}.'
    )
now_utc = datetime.now(timezone.utc)
cutoff_utc = now_utc - timedelta(hours=RUN_HISTORY_HOURS)
enabled_keys = [key for key, _, _ in ENABLED_PRODUCTS]
product_order = {key: idx for idx, (key, _, _, _) in enumerate(PRODUCT_OPTIONS)}
current_product_hours, current_product_snow_ratios = discover_run_product_frames(RUN_OUTPUT_DIR)
current_products = [key for key in enabled_keys if key in current_product_hours]
if not current_products:
    current_products = sorted(
        current_product_hours.keys(),
        key=lambda k: (product_order.get(k, 999), k),
    )
current_hours = sorted({hour for values in current_product_hours.values() for hour in values})
current_ratios = sorted({ratio for values in current_product_snow_ratios.values() for ratio in values})
if not current_ratios and any(k in SNOW_PRODUCT_KEYS for k in current_products):
    current_ratios = SNOW_RATIOS if SNOW_RATIOS else [10]
current_entry = {
    'id': RUN_ID,
    'label': RUN_ID_LABEL,
    'run_date': run_date,
    'init_utc': iso_utc(RUN_INIT_UTC),
    'hours': current_hours,
    'products': current_products,
    'snow_ratios': current_ratios if current_ratios else [10],
    'product_hours': {key: current_product_hours[key] for key in current_products if key in current_product_hours},
    'product_snow_ratios': {
        key: current_product_snow_ratios[key]
        for key in current_products
        if key in current_product_snow_ratios
    },
    'enabled_products': enabled_product_keys,
    'missing_products': missing_product_keys,
    'failed_products': sorted([key for key in failed_product_keys if key in enabled_product_keys]),
    'failure_messages': {
        key: failed_product_messages[key]
        for key in sorted(failed_product_messages.keys())
        if key in failed_product_messages
    },
    'successful_exports': int(successful_exports),
    'failed_exports': len(failures),
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

    discovered_hours, discovered_ratios = discover_run_product_frames(run_dir) if run_dir.exists() else ({}, {})
    product_hours = discovered_hours or normalize_product_hours_map(entry.get('product_hours'))
    product_snow_ratios = discovered_ratios or normalize_product_ratio_map(entry.get('product_snow_ratios'))

    legacy_products = [str(p) for p in entry.get('products', []) if isinstance(p, str)]
    legacy_hours = normalize_int_list(entry.get('hours', []), min_value=0)
    if not product_hours and legacy_products and legacy_hours:
        for key in legacy_products:
            product_hours[key] = list(legacy_hours)

    if not product_snow_ratios:
        legacy_ratios = normalize_int_list(entry.get('snow_ratios', []), min_value=10, max_value=20)
        if legacy_ratios:
            for key in product_hours.keys():
                if is_snow_product(key):
                    product_snow_ratios[key] = list(legacy_ratios)

    products = sorted(
        [key for key, hours in product_hours.items() if hours],
        key=lambda k: (product_order.get(k, 999), k),
    )
    if not products:
        continue
    product_hours = {key: product_hours[key] for key in products if key in product_hours}
    product_snow_ratios = {key: product_snow_ratios[key] for key in products if key in product_snow_ratios}

    hours = sorted({hour for key in products for hour in product_hours.get(key, [])})
    if not hours:
        continue

    snow_ratios = sorted({ratio for key in products for ratio in product_snow_ratios.get(key, [])})
    if not snow_ratios and any(is_snow_product(key) for key in products):
        snow_ratios = [10]
    merged[rid] = {
        'id': rid,
        'label': str(entry.get('label') or rid),
        'run_date': str(entry.get('run_date') or rid),
        'init_utc': iso_utc(init_dt),
        'hours': hours,
        'products': products,
        'snow_ratios': snow_ratios,
        'product_hours': product_hours,
        'product_snow_ratios': product_snow_ratios,
        'enabled_products': [str(p) for p in entry.get('enabled_products', []) if isinstance(p, str)],
        'missing_products': [str(p) for p in entry.get('missing_products', []) if isinstance(p, str)],
        'failed_products': [str(p) for p in entry.get('failed_products', []) if isinstance(p, str)],
        'failure_messages': {
            str(raw_key): str(raw_msg)
            for raw_key, raw_msg in (entry.get('failure_messages', {}) or {}).items()
            if isinstance(raw_key, str)
        },
        'successful_exports': int(_parse_int(entry.get('successful_exports')) or 0),
        'failed_exports': int(_parse_int(entry.get('failed_exports')) or 0),
        'updated_utc': str(entry.get('updated_utc') or iso_utc(now_utc)),
    }

manifest_runs = sorted(
    merged.values(),
    key=lambda item: parse_iso_utc(item.get('init_utc')) or datetime.min.replace(tzinfo=timezone.utc),
    reverse=True,
)
preferred_default_run_id = choose_default_run_id(manifest_runs, RUN_ID)
valid_run_ids = {item['id'] for item in manifest_runs}
if RUNS_ROOT_DIR.exists():
    for child in RUNS_ROOT_DIR.iterdir():
        if child.is_dir() and child.name not in valid_run_ids:
            shutil.rmtree(child, ignore_errors=True)

manifest_payload = {
    'generated_utc': iso_utc(now_utc),
    'history_hours': RUN_HISTORY_HOURS,
    'default_run_id': preferred_default_run_id,
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
    <title>WeatherNext2 viewer</title>
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
        .wrap {
            max-width:1240px;
            margin:0 auto;
            padding:14px 10px calc(var(--controls-h) + 14px);
        }
        .map-wrap {
            background:#111;
            border:1px solid #4f4f4f;
            height:60vh;
            max-height:calc(100vh - var(--controls-h) - 30px);
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
            .bottom-controls { padding:10px 8px calc(14px + env(safe-area-inset-bottom)); }
            .row { gap:8px; }
            #hourSlider { width:min(960px, 96vw); }
            .map-wrap {
                height:52vh;
                min-height:160px;
                max-height:calc(100vh - var(--controls-h) - 18px);
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

        function getProductHours(run, product) {
            if (!run || !product) return [];
            const scoped = (run.product_hours && typeof run.product_hours === 'object')
                ? run.product_hours[product]
                : null;
            const scopedHours = normalizeIntList(scoped, 0, null);
            if (scopedHours.length) return scopedHours;
            return normalizeIntList(run.hours, 0, null);
        }

        function getProductRatios(run, product) {
            if (!run || !product) return [];
            const scoped = (run.product_snow_ratios && typeof run.product_snow_ratios === 'object')
                ? run.product_snow_ratios[product]
                : null;
            const scopedRatios = normalizeIntList(scoped, 10, 20);
            if (scopedRatios.length) return scopedRatios;
            return normalizeIntList(run.snow_ratios, 10, 20);
        }

        function syncProductScopedControls() {
            const run = getCurrentRun();
            if (!run) {
                activeHours = [];
                sliderEl.max = '0';
                sliderEl.value = '0';
                idx = 0;
                return;
            }
            const product = productEl.value;
            const isSnow = snowProducts.has(product);
            const ratios = isSnow ? getProductRatios(run, product) : [];
            setSelectOptions(ratioEl, ratios.length ? ratios : [10], (ratio) => ratio + ':1');
            const previousHour = activeHours[idx];
            activeHours = getProductHours(run, product);
            if (activeHours.length === 0) {
                idx = 0;
            } else {
                const found = activeHours.indexOf(previousHour);
                idx = found >= 0 ? found : 0;
            }
            sliderEl.max = String(Math.max(0, activeHours.length - 1));
            sliderEl.value = String(idx);
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
            setSelectOptions(productEl, products, (key) => productLabels[key] || key);
            syncProductScopedControls();
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
                const edgeGap = isMobile ? 14 : 12;
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
                const ratioOptions = getProductRatios(run, product);
                if (ratioOptions.length && !ratioOptions.includes(Number(ratioEl.value))) {
                    ratioEl.value = String(ratioOptions[0]);
                }
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
        productEl.addEventListener('change', () => {
            syncProductScopedControls();
            render();
        });
        ratioEl.addEventListener('change', () => {
            syncProductScopedControls();
            render();
        });
        sliderEl.addEventListener('input', () => {
            idx = Number(sliderEl.value);
            render();
        });
        prevBtn.addEventListener('click', () => change(-1));
        nextBtn.addEventListener('click', () => change(1));
        mapEl.addEventListener('load', syncBottomInset);

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

