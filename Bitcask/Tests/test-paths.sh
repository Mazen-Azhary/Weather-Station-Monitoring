#!/usr/bin/env bash
# Quick test: Verify path resolution works
echo "Testing path resolution from Tests directory..."
cd "e:\Koleya\Term 8\Data Intensive Apps\finalProj\Weather-Station-Monitoring\Bitcask\Tests"
echo "Current directory: $(pwd)"
echo "Parent directory Files path should be: ../Files/global.hint"
echo "Verifying parent exists: $(ls -d ../Files 2>/dev/null && echo 'OK' || echo 'NOT FOUND')"
echo "Parent directory listing:"
ls -la ../*.java | head -5
