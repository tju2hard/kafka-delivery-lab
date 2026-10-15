#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=/home/walter/.local/opt/talentai/amazon-corretto-17.0.20.12.1-linux-x64
export PATH="$JAVA_HOME/bin:$PATH"
mvn -q compile dependency:copy-dependencies -DincludeScope=runtime
python3 - <<'PY'
from pathlib import Path
import shutil
root=Path('harbor/delivery-matrix/environment/runtime')
root.mkdir(parents=True,exist_ok=True)
shutil.copytree('target/classes',root/'classes',dirs_exist_ok=True)
shutil.copytree('target/dependency',root/'lib',dirs_exist_ok=True)
PY
