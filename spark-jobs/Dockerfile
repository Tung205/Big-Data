FROM apache/spark-py:latest

USER root

WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["/opt/spark/bin/spark-submit", "/app/batch_feature_pipeline.py"]