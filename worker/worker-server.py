import os
import sys
import json
import time
import shutil
import tempfile
import platform
import subprocess
import requests
import redis
from minio import Minio

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

INPUT_BUCKET = os.getenv("INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "output")
WORK_QUEUE_KEY = os.getenv("WORK_QUEUE_KEY", "toWorker")
LOGGING_QUEUE_KEY = os.getenv("LOGGING_QUEUE_KEY", "logging")

redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"


def log_info(msg):
    print(f"INFO: {msg}", flush=True)
    try:
        redis_client.lpush(LOGGING_QUEUE_KEY, f"{infoKey}:{msg}")
    except Exception:
        pass


def log_debug(msg):
    print(f"DEBUG: {msg}", flush=True)
    try:
        redis_client.lpush(LOGGING_QUEUE_KEY, f"{debugKey}:{msg}")
    except Exception:
        pass


def ensure_bucket(name):
    if not minio_client.bucket_exists(name):
        minio_client.make_bucket(name)


def parse_job(raw):
    try:
        job = json.loads(raw)
        return {
            "songhash": job["songhash"],
            "input_object": job.get("input_object", f'{job["songhash"]}.mp3'),
            "model": job.get("model", "default"),
            "callback": job.get("callback")
        }
    except Exception:
        return {
            "songhash": raw.strip(),
            "input_object": f"{raw.strip()}.mp3",
            "model": "default",
            "callback": None
        }


def find_track_dir(output_root):
    needed = {"bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"}
    for root, _, files in os.walk(output_root):
        if needed.issubset(set(files)):
            return root
    return None


def callback_post(callback, songhash, status, detail=None, tracks=None):
    if not callback:
        return
    payload = {"songhash": songhash, "status": status}
    if detail:
        payload["detail"] = detail
    if tracks:
        payload["tracks"] = tracks
    try:
        if isinstance(callback, str):
            requests.post(callback, json=payload, timeout=10)
        elif isinstance(callback, dict) and "url" in callback:
            extra = callback.get("payload", {})
            extra.update(payload)
            requests.post(callback["url"], json=extra, timeout=10)
    except Exception as e:
        log_debug(f"callback failed: {e}")


def process_song(songhash, input_object, model="default", callback=None):
    workdir = tempfile.mkdtemp(prefix=f"demucs-{songhash}-")
    try:
        input_path = os.path.join(workdir, f"{songhash}.mp3")
        output_root = os.path.join(workdir, "output")
        os.makedirs(output_root, exist_ok=True)

        log_info(f"downloading {INPUT_BUCKET}/{input_object}")
        minio_client.fget_object(INPUT_BUCKET, input_object, input_path)

        cmd = ["python3", "-m", "demucs.separate", "--mp3"]
        if model and model != "default":
            cmd += ["-n", model]
        cmd += ["--out", output_root, input_path]

        log_info("running demucs")
        result = subprocess.run(cmd, capture_output=True, text=True)
        log_debug(result.stdout)
        log_debug(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"demucs failed: {result.stderr}")

        track_dir = find_track_dir(output_root)
        if not track_dir:
            raise RuntimeError("no output tracks found")

        tracks = {}
        for track in ["bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"]:
            local_path = os.path.join(track_dir, track)
            if not os.path.exists(local_path):
                raise RuntimeError(f"missing {track}")

            object_name = f"{songhash}/{track}"
            minio_client.fput_object(
                OUTPUT_BUCKET,
                object_name,
                local_path,
                content_type="audio/mpeg"
            )
            tracks[track.replace(".mp3", "")] = object_name
            log_info(f"uploaded {OUTPUT_BUCKET}/{object_name}")

        callback_post(callback, songhash, "completed", tracks=tracks)
        log_info(f"done {songhash}")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main():
    ensure_bucket(OUTPUT_BUCKET)
    log_info("worker started")

    while True:
        try:
            _, raw = redis_client.brpop(WORK_QUEUE_KEY, timeout=0)
            log_debug(f"job: {raw}")
            job = parse_job(raw)

            try:
                process_song(
                    songhash=job["songhash"],
                    input_object=job["input_object"],
                    model=job["model"],
                    callback=job["callback"]
                )
            except Exception as e:
                log_debug(f"failed {job['songhash']}: {e}")
                callback_post(job["callback"], job["songhash"], "failed", detail=str(e))

        except Exception as e:
            log_debug(f"loop error: {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()