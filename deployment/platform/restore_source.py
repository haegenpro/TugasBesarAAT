"""Guest-side bounded, atomic and restartable restoration into an owned empty volume."""
import hashlib
import fcntl
import io
import json
import os
from pathlib import Path,PurePosixPath
import stat
import sys
import tarfile
import tempfile

LIMIT=64<<20

def require(value,message):
    if not value:raise ValueError(message)

def atomic_json(path,value):
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,delete=False)as stream:
            temporary=Path(stream.name);json.dump(value,stream);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
        descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    finally:
        if temporary is not None:temporary.unlink(missing_ok=True)

def entries(raw):
    result={};total=0
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:')as archive:
        for entry in archive:
            name=PurePosixPath(entry.name);total+=entry.size
            require(len(result)<10000 and total<=LIMIT,'restore quota exceeded')
            require(not name.is_absolute()and'..'not in name.parts and'\\'not in entry.name and str(name)not in('','.','/'),'unsafe restore name')
            require(entry.isdir()or entry.isfile(),'restore only accepts directories and regular files')
            key=name.as_posix();require(key not in result,'duplicate restore path')
            content=archive.extractfile(entry).read()if entry.isfile()else None
            result[key]=content
    for name in list(result):
        for parent in PurePosixPath(name).parents:
            if str(parent)=='.':continue
            key=parent.as_posix();require(key not in result or result[key]is None,'archive file shadows a parent directory');result.setdefault(key,None)
    require(len(result)<=10000,'restore directory quota exceeded')
    return result

def matches(root,expected,uid):
    actual={p.relative_to(root).as_posix():p for p in root.rglob('*')}
    if set(actual)!=set(expected):return False
    for name,content in expected.items():
        path=actual[name];info=path.lstat()
        if path.is_symlink()or info.st_uid!=uid:return False
        if content is None:
            if not stat.S_ISDIR(info.st_mode):return False
        elif not stat.S_ISREG(info.st_mode)or info.st_nlink!=1 or info.st_size!=len(content):return False
        elif hashlib.sha256(path.read_bytes()).digest()!=hashlib.sha256(content).digest():return False
    return True

def restore(data,raw):
    root=Path(data['path'])
    require(root.resolve()==root and root.is_dir()and root.parent.stat().st_uid==os.geteuid(),'restore requires original administrator-owned volume parent')
    lock=root.parent/('.'+root.name+'.restore.lock')
    descriptor=os.open(lock,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    try:
        require(os.fstat(descriptor).st_uid==os.geteuid(),'restore lock belongs to another owner')
        fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        _restore(data,raw)
    finally:os.close(descriptor)

def _restore(data,raw):
    root=Path(data['path']);uid=data['uid'];sha=data['sha256']
    require(len(raw)<=LIMIT and hashlib.sha256(raw).hexdigest()==sha,'restore artifact mismatch')
    expected=entries(raw)
    require(root.resolve()==root and root.is_dir()and root.stat().st_uid==uid,'restore requires original owned volume')
    require(root.parent.stat().st_uid==os.geteuid(),'restore staging parent is not owned by administrator')
    stage=root.parent/('.'+root.name+'.restore-'+sha)
    identity={'path':str(root),'uid':uid,'sha256':sha}
    if stage.exists():
        require(not stage.is_symlink()and stage.is_dir()and stage.stat().st_uid==os.geteuid(),'unowned restore staging directory')
        if not(stage/'intent.json').exists():
            require(not any(stage.iterdir())and not any(root.iterdir()),'restore staging has no original intent')
            atomic_json(stage/'intent.json',identity)
        require(json.loads((stage/'intent.json').read_text())==identity,'restore staging identity differs')
    else:
        require(not any(root.iterdir()),'restore refuses nonempty data without an original intent')
        stage.mkdir(mode=0o700);atomic_json(stage/'intent.json',identity)
    tree=stage/'data'
    if(stage/'complete.json').exists():
        require(json.loads((stage/'complete.json').read_text())==identity and not tree.exists()and matches(root,expected,uid),'completed restore differs from original checkpoint')
        return
    # A lost reply after rename leaves a complete data directory. Verify every
    # byte rather than re-extracting over potentially live/recovered data.
    if any(root.iterdir()):
        require(not tree.exists()and matches(root,expected,uid),'restored data changed or is not the recorded checkpoint')
        atomic_json(stage/'complete.json',identity);return
    if tree.exists():require(not tree.is_symlink()and tree.is_dir(),'unsafe staging tree')
    else:tree.mkdir(mode=0o700)
    # This private staging tree contains no accepted application state. Its
    # partial files can be rebuilt while the original target stays empty.
    for path in tree.rglob('*'):
        require(not path.is_symlink()and(path.is_dir()or path.is_file()),'unsafe staged entry')
        require(path.relative_to(tree).as_posix()in expected,'unknown staged entry')
    for name,content in sorted(expected.items(),key=lambda item:(len(PurePosixPath(item[0]).parts),item[0])):
        path=tree.joinpath(*PurePosixPath(name).parts)
        if content is None:path.mkdir(parents=True,exist_ok=True)
        else:
            require(not path.exists()or(path.is_file()and path.stat().st_nlink==1),'unsafe staged file')
            with path.open('wb')as stream:stream.write(content);stream.flush();os.fsync(stream.fileno())
        os.chown(path,uid,uid);os.chmod(path,0o700 if content is None else 0o600)
    os.chown(tree,uid,uid);os.chmod(tree,0o700)
    require(matches(tree,expected,uid),'staged restore verification failed')
    for directory in [*sorted((p for p in tree.rglob('*')if p.is_dir()),key=lambda p:len(p.parts),reverse=True),tree]:
        descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    require(not any(root.iterdir()),'target changed while restore was staged')
    os.replace(tree,root)
    descriptor=os.open(root.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)
    atomic_json(stage/'complete.json',identity)

if __name__=='__main__':
    try:restore(json.loads(sys.argv[1]),sys.stdin.buffer.read(LIMIT+1))
    except (ValueError,KeyError,OSError,tarfile.TarError):
        print('Restore refused: inspect original intent, owned data, checkpoint and staging directory.',file=sys.stderr);sys.exit(1)
