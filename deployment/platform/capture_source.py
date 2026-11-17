"""Guest-side bounded source-volume capture; invoked only after writer shutdown."""
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time

MAX_BYTES=64<<20
MAX_ENTRIES=10000

def require(value,message):
    if not value:raise ValueError(message)

def capture(data,volume_base=Path('/var/lib/aat/local-pv'),owner_marker=Path('/var/lib/aat/platform-owner.json')):
    cluster=data['cluster_id'];formation=cluster+'-services'
    marker=json.loads(owner_marker.read_text())
    require(marker['cluster_id']==cluster,'guest belongs to another cluster')
    parent=volume_base/formation;root=Path(data['path']);uid=data['uid']
    require(root.parent==parent and root.name==data['claim'] and root.name.startswith(cluster+'-'),'unowned source path')
    require(root.resolve()==root and root.is_dir() and root.stat().st_uid==uid,'source volume ownership/path differs')
    require(json.loads((parent/'.owner.json').read_text())=={'cluster_id':cluster,'formation':formation},'formation ownership differs')
    files=sorted(root.rglob('*'))
    require(len(files)<=MAX_ENTRIES,'source entry quota exceeded')
    before={}
    for path in files:
        info=path.lstat();name=path.relative_to(root).as_posix()
        require(not path.is_symlink() and (stat.S_ISREG(info.st_mode)or stat.S_ISDIR(info.st_mode)),'source contains links or special files')
        require('\\'not in name and info.st_uid==uid,'source entry ownership/name differs')
        require(not path.is_file()or info.st_nlink==1,'source contains hard links')
        before[name]=(info.st_ino,info.st_size,info.st_mtime_ns)
    require(sum(v[1]for k,v in before.items()if(root/k).is_file())<=MAX_BYTES,'source volume exceeds capture quota')
    sqlite_files=[]
    for path in files:
        if path.is_file():
            with path.open('rb')as stream:magic=stream.read(16)
            if magic==b'SQLite format 3\x00':sqlite_files.append(path)
    sidecars={str(p)+suffix for p in sqlite_files for suffix in ('-wal','-shm','-journal')}
    sqlite_evidence=[];archive_bytes=io.BytesIO()
    with tempfile.TemporaryDirectory(prefix='aat-source-snapshot-')as folder:
        snapshots={}
        for index,path in enumerate(sqlite_files):
            snapshot=Path(folder)/str(index)
            source=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)
            destination=sqlite3.connect(snapshot);deadline=time.monotonic()+15
            def progress(status,remaining,total):
                require(time.monotonic()<deadline and total*page_size<=MAX_BYTES,'SQLite snapshot deadline/quota exceeded')
            try:
                page_size=source.execute('PRAGMA page_size').fetchone()[0]
                source.backup(destination,pages=64,progress=progress,sleep=.01)
                require(destination.execute('PRAGMA quick_check').fetchall()==[('ok',)],'SQLite snapshot integrity check failed')
                evidence={'path':path.relative_to(root).as_posix(),'quick_check':'ok'}
                columns={row[1]for row in destination.execute('PRAGMA table_info(history)')}
                if 'sequence'in columns:
                    count,high=destination.execute('SELECT count(*),coalesce(max(sequence),0) FROM history').fetchone()
                    evidence.update(history_rows=count,history_highwater=high)
                sqlite_evidence.append(evidence);snapshots[str(path)]=snapshot
            finally:destination.close();source.close()
        with tarfile.open(fileobj=archive_bytes,mode='w',format=tarfile.USTAR_FORMAT)as archive:
            for path in files:
                if str(path)in sidecars:continue
                name=path.relative_to(root).as_posix();entry=tarfile.TarInfo(name)
                entry.uid=entry.gid=uid;entry.mode=0o700 if path.is_dir()else 0o600
                if path.is_dir():entry.type=tarfile.DIRTYPE;archive.addfile(entry)
                else:
                    original=snapshots.get(str(path),path)
                    with original.open('rb')as stream:raw=stream.read(MAX_BYTES+1)
                    require(len(raw)<=MAX_BYTES,'source file grew beyond quota')
                    entry.size=len(raw);archive.addfile(entry,io.BytesIO(raw))
                require(archive_bytes.tell()<=MAX_BYTES,'source archive exceeds quota')
    # Recheck original files and directory membership after every snapshot. A
    # SQLite -shm file may be touched by the read-only backup connection itself.
    after={p.relative_to(root).as_posix():(p.lstat().st_ino,p.lstat().st_size,p.lstat().st_mtime_ns)for p in root.rglob('*')}
    ignored={p.relative_to(root).as_posix()+'-shm'for p in sqlite_files}
    require({k:v for k,v in before.items()if k not in ignored}=={k:v for k,v in after.items()if k not in ignored},'source changed during capture; keep writers stopped')
    raw=archive_bytes.getvalue();require(len(raw)<=MAX_BYTES,'source archive exceeds quota')
    return {'version':1,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'sqlite':sqlite_evidence,'entries':len(files)-sum(str(p)in sidecars for p in files)},raw

if __name__=='__main__':
    try:
        metadata,raw=capture(json.loads(sys.stdin.buffer.read(16385)))
        sys.stdout.buffer.write(json.dumps(metadata).encode()+b'\n'+raw)
    except (ValueError,KeyError,OSError,sqlite3.Error,tarfile.TarError):
        print('Source capture refused: inspect ownership, quotas, integrity and stopped writers.',file=sys.stderr);sys.exit(1)
