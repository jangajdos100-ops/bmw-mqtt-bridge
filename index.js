const mqtt = require('mqtt');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const SB_URL = process.env.SB_URL;
const SB_KEY = process.env.SB_KEY;
const supabase = createClient(SB_URL, SB_KEY);

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
  'vehicle.drivetrain.lastRemainingRange': 'range_km',\n  'vehicle.drivetrain.fuelSystem.level': 'fuel_pct',
  'vehicle.chassis.axle.row1.wheel.left.tire.pressure': 'tire_pressure_fl_kpa',
  'vehicle.chassis.axle.row1.wheel.right.tire.pressure': 'tire_pressure_fr_kpa',
  'vehicle.chassis.axle.row2.wheel.left.tire.pressure': 'tire_pressure_rl_kpa',
  'vehicle.chassis.axle.row2.wheel.right.tire.pressure': 'tire_pressure_rr_kpa',
  'vehicle.cabin.infotainment.navigation.currentLocation.latitude': 'gps_lat',
  'vehicle.cabin.infotainment.navigation.currentLocation.longitude': 'gps_lon',
};

let mqttClient = null;
let currentIdToken = null;
let currentRefreshToken = process.env.BMW_REFRESH_TOKEN;
let tokenExpiresAt = null;

async function refreshToken() {
  console.log('Refreshing BMW token...');
  const params = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: currentRefreshToken,
    client_id: CLIENT_ID,
    scope: 'authenticate_user openid cardata:api:read cardata:streaming:read'
  });
  const res = await fetch(BMW_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: params.toString()
  });
  const json = await res.json();
  if (!json.id_token) throw new Error('Token refresh failed: ' + JSON.stringify(json));
  currentIdToken = json.id_token;
  currentRefreshToken = json.refresh_token;
  tokenExpiresAt = new Date(Date.now() + json.expires_in * 1000);
  await supabase.from('auth_tokens').upsert({
    gcid: MQTT_USERNAME,
    client_id: CLIENT_ID,
    access_token_enc: json.access_token,
    id_token_enc: json.id_token,
    refresh_token_enc: json.refresh_token,
    scope: json.scope,
    expires_at: tokenExpiresAt.toISOString(),
    updated_at: new Date().toISOString()
  }, { onConflict: 'gcid' });
  console.log('Token refreshed, expires:', tokenExpiresAt);
}

function scheduleTokenRefresh() {
  const msUntilExpiry = tokenExpiresAt - Date.now();
  const refreshIn = Math.max(msUntilExpiry - 5 * 60 * 1000, 10000);
  console.log('Next token refresh in ' + Math.round(refreshIn/60000) + ' minutes');
  setTimeout(async () => {
    try {
      await refreshToken();
      if (mqttClient) mqttClient.end(true, () => connectMQTT());
    } catch (e) {
      console.error('Token refresh error:', e.message);
      setTimeout(scheduleTokenRefresh, 60000);
    }
    scheduleTokenRefresh();
  }, refreshIn);
}

function connectMQTT() {
  console.log('Connecting to BMW MQTT...');
  mqttClient = mqtt.connect('mqtts://' + MQTT_HOST + ':' + MQTT_PORT, {
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

    // Diagnostic: wildcard subscribe
    mqttClient.subscribe('#', { qos: 1 }, (err, granted) => {
      if (err) {
        console.error('[DIAG] Wildcard # subscribe ERROR:', err.message);
      } else {
        console.log('[DIAG] Wildcard # SUBACK:', JSON.stringify(granted));
      }
    });

    // Diagnostic: GCID/# wildcard
    const gcidWildcard = MQTT_USERNAME + '/#';
    mqttClient.subscribe(gcidWildcard, { qos: 1 }, (err, granted) => {
      if (err) {
        console.error('[DIAG] GCID/# subscribe ERROR:', err.message);
      } else {
        console.log('[DIAG] GCID/# SUBACK:', JSON.stringify(granted));
      }
    });

    // Exact VIN topics
    VINS.forEach(vin => {
      const topic = MQTT_USERNAME + '/' + vin;
      mqttClient.subscribe(topic, { qos: 1 }, (err, granted) => {
        if (err) {
          console.error('Subscribe error for ' + vin + ':', err.message);
        } else {
          console.log('Subscribed SUBACK ' + vin + ':', JSON.stringify(granted));
        }
      });
    });
  });

  mqttClient.on('message', async (topic, message) => {
    const ts = new Date().toISOString();
    console.log('[DIAG] MESSAGE RECEIVED at ' + ts);
    console.log('[DIAG] Topic:', topic);
    console.log('[DIAG] Payload:', message.toString());
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
  mqttClient.on('close', () => console.log('MQTT connection closed'));
}

async function processPayload(payload) {
  const vin = payload.vin;
  const eventTimestamp = payload.timestamp;
  const data = payload.data || {};
  const snapshotUpdate = { vin, telemetry_timestamp: eventTimestamp };

  for (const [metricKey, metricData] of Object.entries(data)) {
    const value = metricData.value;
    const unit = metricData.unit || null;
    const metricTimestamp = metricData.timestamp || eventTimestamp;
    const fingerprint = Buffer.from(vin + '|' + metricKey + '|' + metricTimestamp + '|' + value).toString('base64');

    await supabase.from('vehicle_events').upsert({
      vin, topic: payload.topic, metric_key: metricKey,
      metric_value_numeric: typeof value === 'number' ? value : null,
      metric_value_text: typeof value === 'string' ? value : null,
      unit, event_timestamp: metricTimestamp,
      payload_json: payload, fingerprint
    }, { onConflict: 'fingerprint', ignoreDuplicates: true });

    if (METRIC_MAP[metricKey]) snapshotUpdate[METRIC_MAP[metricKey]] = value;
  }

  await supabase.from('vehicle_latest_snapshot').upsert(snapshotUpdate, { onConflict: 'vin'});
  console.log('Processed: ' + vin + ' - ' + Object.keys(data).length + ' metrics');
}

apync function main() {
  await refreshToken();
  scheduleTokenRefresh();
  connectMQTT();
}

main().catch(console.error);
