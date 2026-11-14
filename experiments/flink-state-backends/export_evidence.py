#!/usr/bin/env python3
"""Copy compact observations, never the large inputs/outputs/checkpoint data, into tracked results."""
import pathlib,json,hashlib,sys,subprocess
src=pathlib.Path(sys.argv[1]).resolve();dst=pathlib.Path(sys.argv[2]).resolve();dst.mkdir(parents=True,exist_ok=True)
root=pathlib.Path(__file__).resolve().parents[2]
summary=subprocess.check_output([sys.executable,str(pathlib.Path(__file__).with_name('summarize.py')),str(src)],text=True)
(dst/'summary.json').write_text(summary)
statuses={}
for run in src.iterdir():
 if not run.is_dir() or not (run/'config.json').exists():continue
 statuses[run.name]=json.loads((run/'status.json').read_text()) if (run/'status.json').exists() else {'incomplete':True}
 out=dst/run.name;out.mkdir(exist_ok=True)
 for p in run.iterdir():
  if p.suffix not in ('.json','.jsonl') or not p.is_file():continue
  # Redact executor-local prefixes, preserving relative file identities and exact numbers.
  text=p.read_text().replace(str(root),'$REPO').replace('/tmp/flink-bench-tools/amazon-corretto-17.0.21.0.11-linux-x64','$JAVA_HOME')
  (out/p.name).write_text(text)
 # Hash full retained raw output files; these large files remain local and are reproducible.
 hashes={}
 for p in sorted(run.glob('output-*.bin')):
  with p.open('rb') as f:hashes[p.name]={'bytes':p.stat().st_size,'sha256':hashlib.file_digest(f,'sha256').hexdigest()}
 (out/'output-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n')
(dst/'run-statuses.json').write_text(json.dumps(statuses,indent=2)+'\n')
(dst/'fixtures').mkdir(exist_ok=True)
for p in (src/'fixtures').glob('*.json'):(dst/'fixtures'/p.name).write_text(p.read_text())
if (src/'environment.json').exists():(dst/'environment.json').write_text((src/'environment.json').read_text().replace(str(root),'$REPO'))
manifest={}
for p in pathlib.Path(__file__).resolve().parent.rglob('*'):
 if p.is_file() and 'target' not in p.parts and '__pycache__' not in p.parts:manifest[str(p.relative_to(root))]=hashlib.sha256(p.read_bytes()).hexdigest()
(dst/'source-hashes.json').write_text(json.dumps(manifest,indent=2)+'\n')
