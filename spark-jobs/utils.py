import json
import os
import redis
from google.cloud import storage
from google.cloud import bigquery


def load_config(path="/app/config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def download_from_gcs(bucket_name, source_blob, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob)

    print(f"Downloading gs://{bucket_name}/{source_blob} to {local_path} ...")
    blob.download_to_filename(local_path)
    print("Downloaded successfully.")


def get_redis_client(redis_config):
    return redis.Redis(
        host=redis_config["host"],
        port=int(redis_config["port"]),
        decode_responses=True
    )


def write_profiles_to_redis(account_features_df, recent_features_df, redis_config):
    """
    Redis chỉ lưu profile đã aggregate theo cc_num.
    Không lưu raw train.csv vào Redis.
    """

    r = get_redis_client(redis_config)

    account_rows = account_features_df.collect()
    recent_rows = recent_features_df.collect()

    recent_map = {
        str(row["cc_num"]): row.asDict()
        for row in recent_rows
    }

    count_written = 0

    for row in account_rows:
        data = row.asDict()
        cc_num = str(data["cc_num"])
        recent = recent_map.get(cc_num, {})

        profile = {
            "transaction_count": str(data.get("transaction_count", 0) or 0),
            "total_amount": str(data.get("total_amount", 0) or 0),
            "avg_amount": str(data.get("avg_amount", 0) or 0),
            "min_amount": str(data.get("min_amount", 0) or 0),
            "max_amount": str(data.get("max_amount", 0) or 0),
            "fraud_count": str(data.get("fraud_count", 0) or 0),
            "fraud_rate": str(data.get("fraud_rate", 0) or 0),

            "amount_1d": str(recent.get("amount_1d", 0) or 0),
            "amount_7d": str(recent.get("amount_7d", 0) or 0),
            "amount_30d": str(recent.get("amount_30d", 0) or 0),

            "txn_count_1d": str(recent.get("txn_count_1d", 0) or 0),
            "txn_count_7d": str(recent.get("txn_count_7d", 0) or 0),
            "txn_count_30d": str(recent.get("txn_count_30d", 0) or 0)
        }

        redis_key = f"profile:{cc_num}"
        r.hset(redis_key, mapping=profile)
        count_written += 1

    print(f"Wrote {count_written} account profiles to Redis.")


def ensure_bigquery_dataset(project_id, dataset_id, location="US"):
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
        print(f"BigQuery dataset already exists: {dataset_ref}")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset: {dataset_ref}")


def write_spark_df_to_bigquery(spark_df, project_id, dataset_id, table_name, location="US"):
    """
    Hàm này chỉ dùng cho bảng aggregate nhỏ:
    - account_features
    - category_summary
    - geo_summary
    - gender_summary
    - recent_features

    Không dùng để ghi raw train.csv 343MB.
    """

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    print(f"Writing aggregate table to BigQuery: {table_id}")

    pdf = spark_df.toPandas()

    if pdf.empty:
        print(f"Skip BigQuery table {table_id}: empty DataFrame")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    job = client.load_table_from_dataframe(
        pdf,
        table_id,
        job_config=job_config,
        location=location
    )

    job.result()

    print(f"Wrote {len(pdf)} rows to BigQuery table {table_id}")