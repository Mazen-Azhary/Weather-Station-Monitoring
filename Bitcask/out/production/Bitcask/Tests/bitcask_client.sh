#!/bin/bash
# bitcask_client.sh — Bash wrapper for BitcaskClient
# 
# Usage:
#   ./bitcask_client.sh --view-all
#   ./bitcask_client.sh --view --key=STATION_ID
#   ./bitcask_client.sh --concurrent --clients=100

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PARENT_DIR"

# Ensure Java files are compiled
if [ ! -f "BitcaskClient.class" ]; then
    echo "Compiling Bitcask classes..."
    javac *.java
fi

# Run the client with all arguments
java BitcaskClient "$@"
