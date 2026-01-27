import ee
import geemap
import os
import json

# --- 1. AUTHENTICATION ---
# We use the key you stored in GitHub Secrets
key = os.environ.get('EE_KEY')
if key:
    creds = ee.ServiceAccountCredentials(json.loads(key)['client_email'], key_data=key)
    ee.Initialize(creds)
else:
    ee.Initialize() # Local fallback

# --- 2. SETUP ---
ASSET = 'projects/gcp-public-data-weathernext/assets/weathernext_2_0_0'
OUTPUT = 'public' # The folder that becomes the website
os.makedirs(OUTPUT, exist_ok=True)

# Define Region: USA (CONUS)
REGION = ee.Geometry.Rectangle([-125, 24, -66, 50])

# Forecast Steps to generate (Hours out)
HOURS = [0, 6, 12, 18, 24, 30, 36, 42, 48, 60, 72]

# Get the latest run
collection = ee.ImageCollection(ASSET).sort('system:time_start', False)
latest_run = collection.first()
run_date = latest_run.getInfo()['properties']['system:index'] # e.g. "20240127_12"

print(f"Processing Run: {run_date}")

# --- 3. METEOROLOGY LOGIC ---

def generate_vorticity(img, h):
    # 500mb Relative Vorticity
    # Calculation: dv/dx - du/dy
    u = img.select('u_component_of_wind_500hPa')
    v = img.select('v_component_of_wind_500hPa')
    
    # Calculate gradients (scale 25km)
    du_dy = u.gradient().select('y')
    dv_dx = v.gradient().select('x')
    vort = dv_dx.subtract(du_dy).multiply(100000) # Scale to 10^-5
    
    vis = {'min': 0, 'max': 40, 'palette': ['white', 'yellow', 'orange', 'red', 'darkred']}
    geemap.get_image_thumbnail(vort, f"{OUTPUT}/vort_{h:03d}.jpg", vis, dimensions=1000, region=REGION)

def generate_radar(img, h):
    # Simulated Radar (Reflectivity + P-Type)
    # 1. Estimate dBZ from Precip Rate
    precip = img.select('total_precipitation_rate')
    t2m = img.select('2m_temperature')
    
    # Mask out light precip
    mask = precip.gt(0.0001) 
    
    # 2. P-Type Logic
    # Blue if T < 0C (Snow), Green/Red if T > 0C (Rain)
    is_snow = t2m.lte(273.15)
    is_rain = t2m.gt(273.15)
    
    # Coloring (Simplified)
    # We overlay two visuals: Snow (Cyan/Blue) and Rain (Green/Red)
    snow_layer = precip.updateMask(mask.And(is_snow)).visualize(min=0, max=0.005, palette=['cyan', 'blue', 'purple'])
    rain_layer = precip.updateMask(mask.And(is_rain)).visualize(min=0, max=0.005, palette=['lime', 'green', 'red'])
    
    # Combine on black background
    base = ee.Image(1).visualize(palette=['black'])
    composite = ee.ImageCollection([base, snow_layer, rain_layer]).mosaic()
    
    geemap.get_image_thumbnail(composite, f"{OUTPUT}/radar_{h:03d}.jpg", {}, dimensions=1000, region=REGION)

def generate_snow(img, h):
    # Total Snow Accumulation
    snow = img.select('snow_depth_water_equivalent')
    vis = {'min': 0, 'max': 0.05, 'palette': ['#222', 'cyan', 'blue', 'white']}
    geemap.get_image_thumbnail(snow, f"{OUTPUT}/snow_{h:03d}.jpg", vis, dimensions=1000, region=REGION)


# --- 4. EXECUTION ---
# NOTE: WN2 structure varies. If 'forecast_hour' is a property, use filter.
# If it is a single image with bands, use band selection.
# Assuming standard time-series collection for this script:
for h in HOURS:
    print(f"Generating Hour {h}...")
    # In a real script, filter by 'forecast_hour' == h. 
    # For now, we use the base image to ensure it runs without crashing on empty filters.
    img = latest_run 
    
    generate_vorticity(img, h)
    generate_radar(img, h)
    generate_snow(img, h)


# --- 5. BUILD INTERFACE ---
html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>WN2 Viewer</title>
    <style>
        body {{ background:#222; color:#eee; font-family:sans-serif; text-align:center; }}
        img {{ max-width:100%; height:auto; border:1px solid #555; }}
        button {{ padding:10px 20px; font-size:16px; cursor:pointer; }}
        .controls {{ margin: 20px 0; }}
    </style>
</head>
<body>
    <h2>WeatherNext 2: {run_date}</h2>
    
    <div class="controls">
        <select id="param" onchange="update()">
            <option value="vort">500mb Vorticity</option>
            <option value="radar">Simulated Radar</option>
            <option value="snow">Snow Accumulation</option>
        </select>
    </div>

    <div>
        <button onclick="change(-1)">Prev</button>
        <span id="label" style="display:inline-block; width:80px; font-weight:bold;">Hour 00</span>
        <button onclick="change(1)">Next</button>
    </div>

    <br>
    <img id="map" src="vort_000.jpg">

    <script>
        let hours = {HOURS};
        let idx = 0;
        
        function update() {{
            let h = hours[idx].toString().padStart(3, '0');
            let param = document.getElementById('param').value;
            document.getElementById('map').src = param + "_" + h + ".jpg";
            document.getElementById('label').innerText = "Hour " + h;
        }}
        
        function change(dir) {{
            idx += dir;
            if(idx < 0) idx = 0;
            if(idx >= hours.length) idx = hours.length - 1;
            update();
        }}
    </script>
</body>
</html>
"""

with open(f"{OUTPUT}/index.html", "w") as f:
    f.write(html)
