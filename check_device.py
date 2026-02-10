#!/usr/bin/env python3
"""Quick check for Meshtastic device connectivity"""
import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def check_serial():
    """Check for serial devices"""
    print("\n=== SERIAL DEVICES ===")
    try:
        import serial.tools.list_ports
        ports = list(serial.tools.list_ports.comports())
        usb_ports = [p for p in ports if 'usb' in p.device.lower()]
        if usb_ports:
            for p in usb_ports:
                print(f"  FOUND: {p.device}")
                print(f"         Description: {p.description}")
                print(f"         Manufacturer: {p.manufacturer}")
        else:
            print("  No USB serial devices found")
            print("  -> Plug in device via USB cable")
        return usb_ports
    except ImportError:
        print("  pyserial not installed")
        return []

def check_ble():
    """Check for BLE devices"""
    print("\n=== BLE SCAN (10s) ===")
    try:
        import asyncio
        from bleak import BleakScanner
        
        async def scan():
            devices = await BleakScanner.discover(timeout=10)
            mesh = [d for d in devices if d.name and any(
                kw in d.name.lower() for kw in ['mesh', 't1000', 'sensecap', 'tracker']
            )]
            if mesh:
                for d in mesh:
                    print(f"  FOUND: {d.name} ({d.address})")
            else:
                print("  No Meshtastic BLE devices found")
                print("  -> Wake device (press button) or bring closer")
            return mesh
        
        return asyncio.run(scan())
    except ImportError:
        print("  bleak not installed")
        return []

def check_meshtastic_cli():
    """Try meshtastic CLI"""
    print("\n=== MESHTASTIC CLI ===")
    import subprocess
    try:
        result = subprocess.run(
            [sys.executable.replace('python', '.venv/bin/python'), '-m', 'meshtastic', '--info'],
            capture_output=True, text=True, timeout=15
        )
        if 'Connected' in result.stdout or 'Owner' in result.stdout:
            print("  Device connected!")
            print(result.stdout[:500])
            return True
        else:
            print(f"  {result.stdout[:200]}")
            return False
    except Exception as e:
        print(f"  Error: {e}")
        return False

if __name__ == '__main__':
    print("=" * 50)
    print("MESHTASTIC DEVICE CHECK")
    print("=" * 50)
    
    serial_found = check_serial()
    ble_found = check_ble()
    
    if serial_found or ble_found:
        print("\n=== TESTING CONNECTION ===")
        from meshtastic_interface import MeshtasticReceiver
        try:
            r = MeshtasticReceiver(
                connection_type='auto',
                ble_address='93C61E0F-855D-AECB-05B1-3C5193B22964'
            )
            r.connect()
            print("  SUCCESS! Device connected via:", r.connected_via)
            info = r.get_local_node_info()
            if info:
                print(f"  Node: {info}")
            r.close()
        except Exception as e:
            print(f"  Connection failed: {e}")
    else:
        print("\n" + "=" * 50)
        print("NO DEVICES FOUND")
        print("=" * 50)
        print("\nTroubleshooting:")
        print("1. USB: Plug device in with USB cable")
        print("2. BLE: Press button on device to wake it")
        print("3. T1000-E: May be in deep sleep, try USB")
