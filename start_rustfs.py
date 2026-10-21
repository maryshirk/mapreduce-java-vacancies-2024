import json
import os
from pathlib import Path
import shutil

root = Path(os.environ.get('LAB_RUSTFS_DATA', 'runtime/disks')).resolve()
binary = os.environ.get('RUSTFS_BINARY') or shutil.which('rustfs')
if not binary:
    raise SystemExit('Install RustFS 1.0.1 and set RUSTFS_BINARY to its executable.')
path = os.environ.get('LAB_CREDENTIALS_FILE')
credentials = json.loads(Path(path).read_text()) if path else {}
access = os.environ.get('AWS_ACCESS_KEY_ID', credentials.get('access_key'))
secret = os.environ.get('AWS_SECRET_ACCESS_KEY', credentials.get('secret_key'))
if not access or not secret:
    raise SystemExit('Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY locally.')
env = os.environ.copy()
env.update(RUSTFS_ACCESS_KEY=access, RUSTFS_SECRET_KEY=secret,
           RUSTFS_STORAGE_CLASS_STANDARD='EC:2', RUSTFS_ERASURE_SET_DRIVE_COUNT='4',
           RUSTFS_UNSAFE_BYPASS_DISK_CHECK='true', RUST_LOG='info')
disks = [root/f'disk{i}' for i in range(1, 5)]
for disk in disks:
    disk.mkdir(parents=True, exist_ok=True)
args = [binary, 'server', '--address', '127.0.0.1:9000', '--console-enable',
        '--console-address', '127.0.0.1:9001', *map(str, disks)]
print('RustFS: 4 folder disks, EC:2, local disk-check bypass', flush=True)
os.execvpe(binary, args, env)
