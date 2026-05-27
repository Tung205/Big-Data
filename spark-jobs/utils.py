import os
import json
from google.cloud import storage, bigquery

def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_from_gcs(bucket_name, source_blob, local_path):
    """Tải file từ Google Cloud Storage về container"""
    print(f"[*] Đang tải {source_blob} từ bucket {bucket_name}...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob)
    
    # Chỉ tải nếu file chưa tồn tại để tiết kiệm thời gian và băng thông
    if not os.path.exists(local_path):
        blob.download_to_filename(local_path)
        print(f"[+] Đã tải xong: {local_path}")
    else:
        print(f"[+] File đã tồn tại ở {local_path}, bỏ qua tải lại.")

def ensure_bigquery_dataset(project_id, dataset_id, location="US"):
    """Đảm bảo Dataset đã được tạo trên BigQuery"""
    client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[*] Dataset {dataset_ref} đã tồn tại.")
    except Exception:
        print(f"[*] Đang tạo Dataset mới {dataset_ref}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset, timeout=30)

def write_spark_df_to_bigquery(spark_df, project_id, dataset_id, table_name, location="US"):
    """Đẩy bảng thống kê Aggregate lên BigQuery (Tối ưu cho bảng nhỏ)"""
    print(f"[*] Đang đẩy bảng {table_name} lên BigQuery...")
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    # Ép dữ liệu về Pandas DataFrame để đẩy thẳng qua API (Không cần tải .jar rắc rối)
    pandas_df = spark_df.toPandas()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Ghi đè bảng cũ
    )
    
    job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
    job.result() # Đợi cho tiến trình hoàn tất
    print(f"[+] Thành công! Bảng {table_id} đã có {job.output_rows} dòng.")

def write_profiles_to_redis(account_features, recent_features, redis_config):
    # Hàm này sẽ được hoàn thiện khi ta setup xong Redis cho Flink
    pass