const mqtt = require('mqtt');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

// Supabase
const SB_URL = process.env.SB_URL;
const SB_KEY = process.env.SB_KEY;
const supabase = createClient(SB_URL, SB_KEY);

// BMW MQTT
const MQTT_HOST = 'customer.streaming-cardata.bmwgroup.com';
const MQTT_PORT = 9000;
const MQTT_USERNAME = '23ab99db-701d-4d61-94a0-59ddb23a0b3a';
const CLIENT_ID = '98a7c946-5264-4c77-80f3-a76d3b90598b';
const BMW_TOKEN_URL = 'https://customer.bmwgroup.com/gcdm/oauth/token';

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

let mqttClient = null;
let currentIdToken = null;
let currentRefreshToken = null;
let tokenExpiresAt = null;

// --- TOKEN MANAGEMENT ---
async function loadTokenFromDB() {
  const { data, error } = await supabase
    .from('auth_tokens')
    .select('*')
    .order('updated_at', { ascending: false })
    .limit(1)
    .single();
  if (error || !data) throw new Error('No token in DB: ' + error?.message);
  currentIdToken = data.id_token_enc;
  currentRefreshToken = data.refresh_token_enc;
  tokenExpiresAt = new Date(data.expires_at);
  console.log('Token loaded, expires:', tokenExpiresAt);
}

async function refreshToken() {
  console.log('Refreshing BMW token...');
  const res = await fetch(BMW_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      refresh_token: currentRefreshToken,
      client_id: CLIENT_ID,
      scope: 'authenticate_user openid cardata:api:read cardata:streaming:read'
    })
  });
  const json = await res.json();
  if (!json.id_token) throw new Error('Token refresh failed: ' + JSON.stringify(json));

  currentIdToken = json.id_token;
  currentRefreshToken = json.refresh_token;
  tokenExpiresAt = new Date(Date.now() + json.expires_in * 1000);

  // Uloz do DB
  await supabase.from('auth_tokens').insert({
    gcid: MQTT_USERNAME,
    client_id: CLIENT_ID,
    id_token_enc: json.id_token,
    refresh_token_enc: json.refresh_token,
    scope: json.scope,
    expires_at: tokenExpiresAt.toISOString(),
    refresh_expires_at: new Date(Date.now() + 14 * 24 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date().toISOString()
  });
  console.log('Token refreshed and saved, expires:', tokenExpiresAt);
}

function scheduleTokenRefresh() {
  const msUntilExpiry = tokenExpiresAt - Date.now();
  const refreshIn = Math.max(msUntilExpiry - 5 * 60 * 1000, 10000); // 5 min pred expiráciou
  console.log(`Next token refresh in ${Math.round(refreshIn/60000)} minutes`);
  setTimeout(async () => {
    try {
      await refreshToken();
      // Reconnect MQTT s novym tokenom
      if (mqttClient) {
        mqttClient.end(true, () => connectMQTT());
      }
    } catch (e) {
      console.error('Token refresh error:', e.message);
      setTimeout(scheduleTokenRefresh, 60000); // retry za 1 min
    }
    scheduleTokenRefresh();
  }, refreshIn);
}

// --- MQTT ---
function connectMQTT() {
  console.log('Connecting to BMW MQTT...');
  mqttClient = mqtt.connect(`mqtts://${MQTT_HOST}:${MQTT_PORT}`, {
    username: MQTT_USERNAME,
    password: currentIdToken,
    clientId: 'bmw-bridge-' + Date.now(),
    clean: true,
    protocolVersion: 5,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    rejectUnauthorized: true
  });

  mqttClient.on('connect', () => {
    console.log('MQTT connected!');
    VINS.forEach(vin => {
      mqttClient.subscribe(`${MQTT_USERNAME}/${vin}`, { qos: 1 }, (err) => {
        if (err) console.error(`Subscribe error for ${vin}:`, err.message);
        else console.log(`Subscribed: ${vin}`);
      });
    });
  });

  mqttClient.on('message', async (topic, message) => {
    try {
      const payload = JSON.parse(message.toString());
      await processPayload(payload);
    } catch (e) {
      console.error('Message processing error:', e.message);
    }
  });

  mqttClient.on('error', (err) => console.error('MQTT error:', err.message));
  mqttClient.on('reconnect', () => console.log('MQTT reconnecting...'));
  mqttClient.on('offline', () => console.log('MQTT offline'));
}

// --- DATA PROCESSING ---
async function processPayload(payload) {
  const vin = payload.vin;
  const eventTimestamp = payload.timestamp;
  const data = payload.data || {};

  const snapshotUpdate = { vin, telemetry_timestamp: eventTimestamp };

  for (const [metricKey, metricData] of Object.entries(data)) {
    const value = metricData.value;
    const unit = metricData.unit || null;
    const metricTimestamp = metricData.timestamp || eventTimestamp;

    // Fingerprint
    const fingerprint = Buffer.from(`${vin}|${metricKey}|${metricTimestamp}|${value}`).toString('base64');

    // Insert do vehicle_events
    await supabase.from('vehicle_events').upsert({
      vin,
      topic: payload.topic,
      metric_key: metricKey,
      metric_value_numeric: typeof value === 'number' ? value : null,
      metric_value_text: typeof value === 'string' ? value : null,
      unit,
      event_timestamp: metricTimestamp,
      payload_json: payload,
      fingerprint
    }, { onConflict: 'fingerprint', ignoreDuplicates: true });

    // Mapovanie na snapshot stĺpce
    if (METRIC_MAP[metricKey]) {
      snapshotUpdate[METRIC_MAP[metricKey]] = value;
    }
  }

  // Upsert snapshot
  await supabase.from('vehicle_latest_snapshot').upsert(snapshotUpdate, {
    onConflict: 'vin'
  });

  console.log(`Processed: ${vin} - ${Object.keys(data).length} metrics`);
}

// --- MAIN ---
async function main() {
  await loadTokenFromDB();
  scheduleTokenRefresh();
  connectMQTT();
}

main().catch(console.error);
