import argparse
import base64
import concurrent.futures as cf
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Optional, Tuple

from boxsdk import Client, JWTAuth
from boxsdk.exception import BoxAPIException
from boxsdk.util.chunked_uploader import ChunkedUploader

DIRECT_UPLOAD_MAX = 50 * 1024 * 1024  # 50MB per Box API
DEFAULT_WORKERS = min(8, (os.cpu_count() or 4) * 4)
DEFAULT_RETRIES = 6
DEFAULT_BACKOFF = 0.75  # seconds
DEFAULT_CHUNK = 8 * 1024 * 1024  # 8MB

def load_jwt(jwt_config_file: Optional[str]) -> JWTAuth:
    if jwt_config_file:
        return JWTAuth.from_settings_file(jwt_config_file)
    b64 = os.getenv("JWT_CONFIG_BASE_64")
    if not b64:
        raise RuntimeError("Provide --jwt-config or set JWT_CONFIG_BASE_64.")
    try:
        json_str = base64.b64decode(b64).decode("utf-8")
    except Exception as e:
        raise RuntimeError("JWT_CONFIG_BASE_64 is not valid base64 JSON") from e
    return JWTAuth.from_settings_dictionary(eval(json_str))  # dict expected

def make_client(jwt_config_file: Optional[str], as_user: Optional[str]) -> Client:
    auth = load_jwt(jwt_config_file)
    client = Client(auth)
    return client.as_user(as_user) if as_user else client

def should_skip(p: Path) -> bool:
    if not p.is_file():
        return True
    name = p.name
    return not name or name.startswith(".DS_Store")

def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if should_skip(p):
            continue
        yield p

def is_retryable(e: Exception) -> Tuple[bool, Optional[int], Optional[float]]:
    if isinstance(e, BoxAPIException):
        code = e.status
        if code in (409, 400):  # not retrying these
            return False, code, None
        retry_after = None
        try:
            ra = e.headers.get("Retry-After") if e.headers else None
            retry_after = float(ra) if ra else None
        except Exception:
            retry_after = None
        if code == 429 or (500 <= code <= 599):
            return True, code, retry_after
        return False, code, None
    return True, None, None  # network/other: retry

def backoff_sleep(attempt: int, base: float, retry_after: Optional[float]) -> None:
    delay = retry_after if retry_after is not None else base * (2 ** (attempt - 1))
    # jitter in [0.75, 1.25)
    jitter = 0.75 + 0.5 * (os.urandom(1)[0] / 255.0)
    time.sleep(delay * jitter)

def upload_small(client: Client, folder_id: str, file_path: Path):
    folder = client.folder(folder_id)
    return folder.upload(file_path=str(file_path), file_name=file_path.name)

def upload_large(client: Client, folder_id: str, file_path: Path, chunk_size: int):
    size = file_path.stat().st_size
    folder = client.folder(folder_id)
    session = folder.create_upload_session(file_size=size, file_name=file_path.name)
    with file_path.open("rb") as f:
        uploader = ChunkedUploader(session=session, file_stream=f, chunk_size=chunk_size)
        return uploader.start()

def upload_with_retries(
    client: Client,
    folder_id: str,
    file_path: Path,
    retries: int,
    base_backoff: float,
    chunk_size: int,
) -> Tuple[Path, Optional[str], Optional[str]]:
    attempt = 0
    while True:
        try:
            if file_path.stat().st_size <= DIRECT_UPLOAD_MAX:
                file_obj = upload_small(client, folder_id, file_path)
            else:
                file_obj = upload_large(client, folder_id, file_path, chunk_size)
            return file_path, file_obj.id, None
        except Exception as e:
            retryable, code, retry_after = is_retryable(e)
            if isinstance(e, BoxAPIException) and code == 409:
                return file_path, None, "skipped: 409 Conflict (name exists)"
            if isinstance(e, BoxAPIException) and code == 400:
                return file_path, None, "skipped: 400 Bad Request"
            if not retryable:
                return file_path, None, f"failed: HTTP {code or 'n/a'} {type(e).__name__}"
            attempt += 1
            if attempt > retries:
                return file_path, None, f"failed after {retries} retries: {type(e).__name__}"
            backoff_sleep(attempt, base_backoff, retry_after)

def main() -> int:
    ap = argparse.ArgumentParser(description="Upload a folder to Box using legacy boxsdk[jwt] with concurrency and retries")
    ap.add_argument("local_folder")
    ap.add_argument("box_folder_id")
    ap.add_argument("--jwt-config", help="Path to Box app JWT JSON (or set JWT_CONFIG_BASE_64)")
    ap.add_argument("--as-user", help="Act-as user ID (optional)")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    ap.add_argument("--backoff", type=float, default=DEFAULT_BACKOFF)
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK, help="Chunk size for large files")
    args = ap.parse_args()

    root = Path(args.local_folder).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"invalid local folder: {root}", file=sys.stderr)
        return 2

    client = make_client(args.jwt_config, args.as_user)

    files = list(iter_files(root))
    if not files:
        print("no files to upload")
        return 0

    print(f"Uploading {len(files)} files to Box folder {args.box_folder_id} with {args.workers} workers...")
    ok = 0
    skipped = 0
    failed = 0

    def task(p: Path):
        return upload_with_retries(client, args.box_folder_id, p, args.retries, args.backoff, args.chunk)

    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        for path, file_id, err in ex.map(task, files):
            if err is None and file_id:
                ok += 1
                print(f"[OK] {path} -> file_id={file_id}")
            elif err and err.startswith("skipped"):
                skipped += 1
                print(f"[SKIP] {path} -> {err}")
            else:
                failed += 1
                print(f"[ERR] {path} -> {err or 'unknown error'}")

    print(f"Done. ok={ok} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
