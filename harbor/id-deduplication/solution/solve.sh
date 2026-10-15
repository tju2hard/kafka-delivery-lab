#!/bin/bash
set -euo pipefail
python3 /app/deduplication.py --output /app/deduplication-report.json
