#!/usr/bin/env python3
"""
bitcask_client.py — Python wrapper for BitcaskClient

Usage:
    python bitcask_client.py --view-all
    python bitcask_client.py --view --key=STATION_ID
    python bitcask_client.py --concurrent --clients=100
"""

import subprocess
import sys
import os

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    os.chdir(parent_dir)
    
    # Check if Java files are compiled
    if not os.path.exists("BitcaskClient.class"):
        print("Compiling Bitcask classes...")
        result = subprocess.run(["javac", "*.java"], shell=True)
        if result.returncode != 0:
            print("Compilation failed", file=sys.stderr)
            sys.exit(1)
    
    # Run the Java client with all arguments
    cmd = ["java", "BitcaskClient"] + sys.argv[1:]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
