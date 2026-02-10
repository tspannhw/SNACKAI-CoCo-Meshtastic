#!/usr/bin/env python3
"""Send test sensor data to Slack"""
import json
import requests
from datetime import datetime

with open('snowflake_config.json') as f:
    config = json.load(f)

webhook = config['slack']['webhook_url']

def send(msg):
    r = requests.post(webhook, json={"text": msg}, timeout=10)
    print(f"Sent: {r.status_code}")
    return r.status_code == 200

device_id = "!4b14test"
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

env_msg = f"""🌡️ *Environmental Sensors*
Device: `{device_id}`
• Temperature: 22.5°C (72.5°F)
• Humidity: 45.2%
• Pressure: 1013.25 hPa
• Air Quality (IAQ): 85
• Time: {now}"""

pos_msg = f"""📍 *Position Update*
Device: `{device_id}`
• Location: 40.758896, -73.985130
• Altitude: 15m
• Speed: 1.2 m/s
• Heading: 270°
• Satellites: 12
• Map: https://maps.google.com/?q=40.758896,-73.985130
• Time: {now}"""

dev_msg = f"""📊 *Device Metrics*
Device: `{device_id}`
• Battery: 🟢 75%
• Voltage: 3.95V
• Channel Util: 12.5%
• Air Util TX: 3.2%
• Uptime: 48h 23m
• SNR: 9.5 dB
• RSSI: -85 dBm
• Time: {now}"""

text_msg = f"""💬 *Text Message*
From: `{device_id}`
Message: Hello from the mesh network!
• SNR: 8.0 dB
• Time: {now}"""

battery_msg = f"""🔋 *Low Battery Alert*
Device: `{device_id}`
• Battery: 15%
• Voltage: 3.45V
• Time: {now}"""

print("Sending test messages to Slack...")
send(env_msg)
send(pos_msg)
send(dev_msg)
send(text_msg)
send(battery_msg)
print("Done! Check your #meshtastic-alerts channel")
