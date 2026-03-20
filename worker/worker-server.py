import os
import sys
import json
import shutil
import tempfile
import platform
import requests
import subprocess

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

infoKey = "{}.worker.info".format(platform.node())
debugKey = "{}.worker.debug".format(platform.node())


def log_debug(message, key=debugKey):
    print("DEBUG:", message, file=sys.stdout)
    try:
        redisClient = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redisClient.lpush("logging", f"{key}:{message}")
    except Exception as exp:
        print(f"DEBUG logging failed: {str(exp)}", file=sys.stdout)


def log_info(message, key=infoKey):
    print("INFO:", message, file=sys.stdout)
    try:
        redisClient = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redisClient.lpush("logging", f"{key}:{message}")
    except Exception as exp:
        print(f"INFO logging failed: {str(exp)}", file=sys.stdout)


def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        log_info(f"Created bucket {bucket_name}")


def maybe_call_callback(callback, songhash, status, detail=None):
    if not callback:
        return

    try:
        payload = {
            "hash": songhash,
            "status": status
        }
        if detail is not None:
            payload["detail"] = detail

        if isinstance(callback, str):
            requests.post(callback, json=payload, timeout=5)
        elif isinstance(callback, dict) and "url" in callback:
            callback_payload = callback.get("payload", {})
            callback_payload.update(payload)
            requests.post(callback["url"], json=callback_payload, timeout=5)

        log_info(f"Callback attempted for {songhash} with status {status}")
    except Exception as exp:
        log_debug(f"Callback failed for {songhash}: {str(exp)}")


def process_song(songhash: str, model: str = "default", callback=None):
    log_info(f"Processing songhash={songhash} model={model}")

    workdir = tempfile.mkdtemp(prefix=f"demucs-{songhash}-")
    try:
        input_path = os.path.join(workdir, f"{songhash}.mp3")
        output_root = os.path.join(workdir, "output")
        os.makedirs(output_root, exist_ok=True)

        log_info(f"Downloading input object {INPUT_BUCKET}/{songhash}.mp3")
        minio_client.fget_object(INPUT_BUCKET, f"{songhash}.mp3", input_path)

        cmd = [
            "python3", "-m", "demucs.separate",
            "--mp3",
            "--out", output_root,
            input_path
        ]
        log_info(f"Running demucs for {songhash}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(f"demucs failed: {result.stderr}")

        model_dirs = [
            d for d in os.listdir(output_root)
            if os.path.isdir(os.path.join(output_root, d))
        ]
        if not model_dirs:
            raise RuntimeError("No model output directory found")

        model_dir = os.path.join(output_root, model_dirs[0], songhash)
        if not os.path.isdir(model_dir):
            raise RuntimeError(f"Expected output directory not found: {model_dir}")

        for track in ["bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"]:
            local_track_path = os.path.join(model_dir, track)
            if not os.path.exists(local_track_path):
                raise RuntimeError(f"Missing output track: {local_track_path}")

            object_name = f"{songhash}-{track}"
            minio_client.fput_object(
                OUTPUT_BUCKET,
                object_name,
                local_track_path,
                content_type="audio/mpeg"
            )
            log_info(f"Uploaded output track {object_name}")

        log_info(f"Finished processing {songhash}")
        maybe_call_callback(callback, songhash, "completed")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        log_debug(f"Cleaned temporary directory for {songhash}")


def main():
    ensure_bucket(INPUT_BUCKET)
    ensure_bucket(OUTPUT_BUCKET)

    log_info("Worker started and waiting for jobs")

    while True:
        try:
            item = redis_client.brpop(WORK_QUEUE_KEY, timeout=0)
            if not item:
                continue

            queue_name, raw_job = item
            log_debug(f"Received raw job from {queue_name}: {raw_job}")

            try:
                job = json.loads(raw_job)
                songhash = job["songhash"]
                model = job.get("model", "default")
                callback = job.get("callback")
            except Exception:
                songhash = raw_job
                model = "default"
                callback = None

            log_info(f"Dequeued job for {songhash}")

            try:
                process_song(songhash, model=model, callback=callback)
            except Exception as exp:
                log_debug(f"Failed processing {songhash}: {str(exp)}")
                maybe_call_callback(callback, songhash, "failed", str(exp))

        except Exception as exp:
            log_debug(f"Exception in worker loop: {str(exp)}")


if __name__ == "__main__":
    main()