import os
import io
import sys
import json
import base64
import hashlib
import platform

import redis
from flask import Flask, request, jsonify, Response
from minio import Minio

app = Flask(__name__)

redisHost = os.getenv("REDIS_HOST", "localhost")
redisPort = int(os.getenv("REDIS_PORT", "6379"))

redis_client = redis.StrictRedis(
    host=redisHost,
    port=redisPort,
    db=0,
    decode_responses=True
)

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)

INPUT_BUCKET = "queue"
OUTPUT_BUCKET = "output"
WORK_QUEUE_KEY = "toWorker"
CALLBACK_LOG_KEY = "completedCallbacks"
REST_CALLBACK_URL = os.getenv("REST_CALLBACK_URL", "http://rest:5000/apiv1/callback")

ALLOWED_TRACKS = {"base.mp3", "bass.mp3", "vocals.mp3", "drums.mp3", "other.mp3"}

infoKey = "{}.rest.info".format(platform.node())
debugKey = "{}.rest.debug".format(platform.node())


def log_debug(message, key=debugKey):
    print("DEBUG:", message, file=sys.stdout)
    try:
        redis_client.lpush("logging", f"{key}:{message}")
    except Exception as exp:
        print(f"DEBUG logging failed: {str(exp)}", file=sys.stdout)


def log_info(message, key=infoKey):
    print("INFO:", message, file=sys.stdout)
    try:
        redis_client.lpush("logging", f"{key}:{message}")
    except Exception as exp:
        print(f"INFO logging failed: {str(exp)}", file=sys.stdout)


def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        log_info(f"Created bucket {bucket_name}")


@app.route("/", methods=["GET"])
def hello():
    return "<h1>Music Separation Server</h1><p>Use a valid endpoint</p>"


@app.route("/apiv1/callback", methods=["POST"])
def callback():
    """
    Worker finishes processing and POSTs callback data here.
    We log it and store it in Redis for inspection.
    """
    data = request.get_json(silent=True) or {}
    songhash = data.get("songhash", "unknown")

    try:
        redis_client.hset(CALLBACK_LOG_KEY, songhash, json.dumps(data))
        log_info(f"Received callback for {songhash}: {json.dumps(data)}")
        return jsonify({"status": "ok", "songhash": songhash}), 200
    except Exception as exp:
        log_debug(f"Callback processing failed: {str(exp)}")
        return jsonify({"error": str(exp)}), 500


@app.route("/apiv1/separate", methods=["POST"])
def separate():
    data = request.get_json(silent=True)
    if not data or "mp3" not in data:
        log_debug("Missing mp3 field in request")
        return jsonify({"error": "missing mp3"}), 400

    mp3_b64 = data["mp3"]
    model = data.get("model", "default")

    incoming_callback = data.get("callback", {})
    if isinstance(incoming_callback, dict):
        callback_data = incoming_callback.get("data", {})
    else:
        callback_data = {}

    try:
        mp3_bytes = base64.b64decode(mp3_b64)
    except Exception as exp:
        log_debug(f"Invalid base64 payload: {str(exp)}")
        return jsonify({"error": "invalid base64"}), 400

    songhash = hashlib.sha224(mp3_bytes).hexdigest()
    log_info(f"Received separation request for {songhash}")

    callback = {
        "url": REST_CALLBACK_URL,
        "data": {
            "songhash": songhash,
            "model": model,
            **callback_data
        }
    }

    try:
        ensure_bucket(INPUT_BUCKET)
        ensure_bucket(OUTPUT_BUCKET)

        minio_client.put_object(
            INPUT_BUCKET,
            f"{songhash}.mp3",
            io.BytesIO(mp3_bytes),
            length=len(mp3_bytes),
            content_type="audio/mpeg"
        )
        log_info(f"Uploaded input object {songhash}.mp3 to bucket {INPUT_BUCKET}")

        job = {
            "songhash": songhash,
            "model": model,
            "callback": callback
        }

        redis_client.lpush(WORK_QUEUE_KEY, json.dumps(job))
        log_info(f"Enqueued work item for {songhash} on queue {WORK_QUEUE_KEY}")

        return jsonify({
            "hash": songhash,
            "reason": "Song enqueued for separation",
            "callback_url": REST_CALLBACK_URL
        }), 200

    except Exception as exp:
        log_debug(f"Error in /apiv1/separate for {songhash}: {str(exp)}")
        return jsonify({"error": str(exp)}), 500


@app.route("/apiv1/queue", methods=["GET"])
def queue():
    try:
        q = redis_client.lrange(WORK_QUEUE_KEY, 0, -1)
        log_debug(f"Queue inspected, {len(q)} item(s) found")
        return jsonify({"queue": q})
    except Exception as exp:
        log_debug(f"Queue inspection failed: {str(exp)}")
        return jsonify({"error": str(exp)}), 500


@app.route("/apiv1/callbacks", methods=["GET"])
def callbacks():
    try:
        items = redis_client.hgetall(CALLBACK_LOG_KEY)
        return jsonify(items), 200
    except Exception as exp:
        log_debug(f"Callback inspection failed: {str(exp)}")
        return jsonify({"error": str(exp)}), 500


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def track(songhash, track):
    if track not in ALLOWED_TRACKS:
        log_debug(f"Invalid track requested: {track}")
        return jsonify({"error": "invalid track"}), 400

    object_name = f"{songhash}/{track}"

    try:
        obj = minio_client.get_object(OUTPUT_BUCKET, object_name)
        data = obj.read()
        obj.close()
        obj.release_conn()

        log_info(f"Returned track {object_name}")
        return Response(
            data,
            mimetype="audio/mpeg",
            headers={"Content-Disposition": f'attachment; filename="{track}"'}
        )
    except Exception as exp:
        log_debug(f"Track retrieval failed for {object_name}: {str(exp)}")
        return jsonify({"error": "track not found"}), 404


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET"])
def remove(songhash, track):
    if track not in ALLOWED_TRACKS:
        log_debug(f"Invalid track removal requested: {track}")
        return jsonify({"error": "invalid track"}), 400

    object_name = f"{songhash}/{track}"

    try:
        minio_client.remove_object(OUTPUT_BUCKET, object_name)
        log_info(f"Removed track {object_name}")
        return jsonify({"removed": object_name}), 200
    except Exception as exp:
        log_debug(f"Track removal failed for {object_name}: {str(exp)}")
        return jsonify({"error": "track not found"}), 404


if __name__ == "__main__":
    ensure_bucket(INPUT_BUCKET)
    ensure_bucket(OUTPUT_BUCKET)
    log_info("REST server starting on port 5000")
    app.run(host="0.0.0.0", port=5000, debug=True)