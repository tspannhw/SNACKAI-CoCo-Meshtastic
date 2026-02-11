# Deploy to Personal iPhone - Developer Mode Guide

This guide explains how to install MeshtasticMobile directly on your personal iPhone for testing.

## Prerequisites

- macOS 13.0+ with Xcode 15+
- iPhone with iOS 16+ 
- USB cable (Lightning or USB-C)
- Apple ID (free or paid developer account)

---

## Step 1: Enable Developer Mode on iPhone

**iOS 16 and later requires Developer Mode to be enabled:**

1. Open **Settings** on your iPhone
2. Go to **Privacy & Security**
3. Scroll to the bottom and tap **Developer Mode**
4. Toggle **Developer Mode** ON
5. Tap **Restart** when prompted
6. After restart, tap **Turn On** and enter your passcode

> ⚠️ If you don't see Developer Mode, connect your iPhone to a Mac with Xcode first.

---

## Step 2: Trust Your Mac

1. Connect iPhone to Mac via USB cable
2. **Unlock your iPhone**
3. Tap **Trust** on the "Trust This Computer?" prompt
4. Enter your iPhone passcode
5. Wait for Xcode to recognize the device

**Verify connection:**
```bash
./Scripts/deploy-device.sh --list
```

---

## Step 3: Configure Apple ID in Xcode

### Using Xcode UI:
1. Open **Xcode**
2. Go to **Xcode → Settings → Accounts** (⌘,)
3. Click **+** in the bottom left
4. Select **Apple ID**
5. Sign in with your Apple ID

### Find Your Team ID:
1. In Xcode Accounts, select your Apple ID
2. Look for your **Team** (Personal Team or Organization)
3. The Team ID is a 10-character string (e.g., `ABC123XYZ0`)

Or visit: https://developer.apple.com/account → Membership → Team ID

---

## Step 4: Deploy to Device

### Quick Deploy (Automatic Signing):
```bash
cd /Users/tspann/Downloads/code/coco/meshtastic/MeshtasticMobile
./Scripts/deploy-device.sh
```

### With Explicit Team ID:
```bash
export APPLE_TEAM_ID="YOUR_TEAM_ID"
./Scripts/deploy-device.sh
```

### Via Xcode UI:
1. Open project in Xcode:
   ```bash
   open MeshtasticMobile.xcodeproj
   ```
2. Select your iPhone from the device dropdown (top bar)
3. Click **Run** (▶) or press `⌘R`

---

## Step 5: Trust Developer Certificate on iPhone

After first install, you must trust the developer certificate:

1. Open **Settings** on iPhone
2. Go to **General → VPN & Device Management**
3. Under "Developer App", tap your Apple ID email
4. Tap **Trust "[your email]"**
5. Tap **Trust** to confirm

Now you can open the app!

---

## Free vs Paid Developer Account

| Feature | Free Account | Paid ($99/year) |
|---------|--------------|-----------------|
| Install on device | ✅ Yes | ✅ Yes |
| App validity | 7 days | 1 year |
| Max apps | 3 | Unlimited |
| Push notifications | ❌ No | ✅ Yes |
| TestFlight | ❌ No | ✅ Yes |
| App Store | ❌ No | ✅ Yes |

**With a free account**, you'll need to reinstall the app every 7 days.

---

## Wireless Debugging (Optional)

After initial USB setup, you can deploy wirelessly:

### Enable in Xcode:
1. Connect device via USB
2. Open **Window → Devices and Simulators**
3. Select your iPhone
4. Check **Connect via network**
5. Disconnect USB cable

### Or via command line:
```bash
./Scripts/deploy-device.sh --wireless
```

Now deploy without USB:
```bash
./Scripts/deploy-device.sh
```

---

## Troubleshooting

### "Unable to install app"
- Ensure Developer Mode is enabled
- Trust the developer certificate in Settings
- Try restarting both iPhone and Xcode

### "Device not found"
```bash
# Check USB connection
./Scripts/deploy-device.sh --list

# Verify device is trusted
# Disconnect and reconnect USB
# Tap "Trust" on iPhone
```

### "Signing failed"
- Open Xcode and sign in with Apple ID
- In project settings, set Team to your Apple ID
- Let Xcode manage signing automatically

### "App crashes on launch"
- Check Xcode console for errors
- Ensure iOS version is 16.0+
- Try clean build: `./Scripts/build.sh clean`

### "Untrusted Developer"
1. Settings → General → VPN & Device Management
2. Tap your developer email
3. Tap "Trust"

---

## Quick Reference

```bash
# List connected devices
./Scripts/deploy-device.sh --list

# Deploy to device
./Scripts/deploy-device.sh

# Deploy with Team ID
APPLE_TEAM_ID=ABC123XYZ0 ./Scripts/deploy-device.sh

# Setup wireless debugging
./Scripts/deploy-device.sh --wireless

# Show full setup guide
./Scripts/deploy-device.sh --setup

# Open in Xcode
open MeshtasticMobile.xcodeproj
```

---

## Alternative: Install via Xcode

1. Open terminal:
   ```bash
   cd /Users/tspann/Downloads/code/coco/meshtastic/MeshtasticMobile
   open MeshtasticMobile.xcodeproj
   ```

2. In Xcode:
   - Select your iPhone from device picker (top toolbar)
   - Click **Run** (▶) or press `⌘R`
   - First time: Xcode will prompt to fix signing issues
   - Click **Fix Issue** and sign in with Apple ID

3. On iPhone:
   - Trust developer certificate (Settings → General → VPN & Device Management)
   - Open the app
