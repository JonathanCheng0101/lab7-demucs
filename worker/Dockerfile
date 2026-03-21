FROM xserrat/facebook-demucs:latest

WORKDIR /app

RUN pip3 install --no-cache-dir minio redis requests

COPY worker-server.py /app/worker-server.py

CMD ["python3", "/app/worker-server.py"]
