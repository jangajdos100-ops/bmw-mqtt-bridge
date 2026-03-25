const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const SB_URL = process.env.SB_URL;
const SB_KEY = process.env.SB_KEY;
const supabase = createClient(SB_URL, SB_KEY);

const CLIENT_ID = '98a7c946-5264-4c77-80f3-a76d3b90598b';
const BMW_TOKEN_URL = 'https://customer.bmwgroup.com/gcdm/oauth/token';
const REST_BASE = 'https://api-cardata.bmwgroup.com';
const CONTAINER_ID = process.env.BMW_CONTAINER_ID;

const VINS = [
  'WBA31FZ070FP41983','WBA61CA0809N20642','WBA71GP0309186068',
  'WBA48FU0808D91827','WBA31FZ010FR12646','WBA31DY040FN13574',
  'WBA31DZ020FM53115','WBAYL110205U37030','WBA31DY010FM46383',
  'WBA31AX030FR91176','WBA8J11020A968984','WBA21EN0609U13979',
  'WBA11FJ0X0CR14240'
];

const METRIC_MAP = {
  'vehicle.vehicle.travelledDistance': 'odometer_km',
  'vehicle.drivetrain.lastRemainingRange': 'range_km',
  'vehicle.drivetrain.fuelSystem.level': 'fuel_pct',
  'vehicle.chassis.axle.row1.wheel.left.tire.pressure': 'tire_pressure_fl_kpa',
  'vehicle.chassis.axle.row1.wheel.right.tire.pressure': 'tire_pressure_fr_kpa',
  'vehicle.chassis.axle.row2.wheel.left.tire.pressure': 'tire_pressure_rl_kpa',
  'vehicle.chassis.axle.row2.wheel.right.tire.pressure': 'tire_pressure_rr_kpa',
  'vehicle.cabin.infotainment.navigation.currentLocation.latitude': 'gps_lat',
  'vehicle.cabin.infotainment.navigation.currentLocation.longitude': 'gps_lon',
};

async function getAccessToken() {
  const { data } = await supabase
    .from('auth_tokens')
    .select('refresh_token_enc')
    .single();

  const params = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: data.refresh_token_enc,
    client_id: CLIENT_ID,
    scope: 'authenticate_user openid cardata:api:read cardata:streaming:read'
  });

  const res = await fetch(BMW_TOKEN_URL + '?' + params.toString(), {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
  });
  const json = await res.json();
  if (!json.access_token) throw new Error('Token failed: ' + JSON.stringify(json));

  // Update refresh token in Supabase
  await supabase.from('auth_tokens').update({
    refresh_token_enc: json.refresh_token,
    updated_at: new Date().toISOString()
  }).eq('client_id', CLIENT_ID);

  return json.access_token;
}

async function pollVin(vin, accessToken) {
  const url = `${REST_BASE}/v1/containers/${CONTAINER_ID}/vehicles/${vin}/telematicData`;
  const res = await fetch(url, {
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'x-version': 'v1'
    }
  });

  if (!res.ok) {
    console.error(`REST error for ${vin}: ${res.status}`);
    return;
  }

  const json = await res.json();
  const metrics = json.data || {};
  const snapshotUpdate = { vin, telemetry_timestamp: new Date().toISOString(), data };

  for (const [metricKey, metricData] of Object.entries(metrics)) {
    const value = metricData.value;
    if (METRIC_MAP[metricKey]) snapshotUpdate[METRIC_MAP[metricKey]] = value;
  }

  await supabase.from('vehicle_latest_snapshot').upsert(snapshotUpdate, { onConflict: 'vin' });
  console.log(`Polled: ${vin} - ${Object.keys(metrics).length} metrics`);
}

async function main() {
  console.log('REST poll started:', new Date().toISOString());
  const accessToken = await getAccessToken();
  for (const vin of VINS) {
    await pollVin(vin, accessToken);
  }
  console.log('REST poll done');
}

main().catch(console.error);
