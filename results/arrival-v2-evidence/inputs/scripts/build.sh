#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=/home/walter/.local/opt/talentai/amazon-corretto-17.0.20.12.1-linux-x64
export PATH="$JAVA_HOME/bin:$PATH"
mvn -q clean compile dependency:copy-dependencies -DincludeScope=runtime
python3 - <<'PY'
from pathlib import Path
import shutil
for task in ('delivery-matrix','id-marking-latency','id-marking-recovery','id-deduplication','streams-id-filter','arrival-marking-comparison'):
 root=Path('harbor')/task/'environment/runtime'
 if root.exists():shutil.rmtree(root)
 root.mkdir(parents=True)
 shutil.copytree('target/classes',root/'classes',dirs_exist_ok=True)
 shutil.copytree('target/dependency',root/'lib',dirs_exist_ok=True)
PY
