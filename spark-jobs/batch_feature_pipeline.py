from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, from_json, current_timestamp, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)
import os

# Cấu hình dự án (Thay bằng Project ID và Dataset ID thật của em)
GCP_PROJECT_ID = "project-7942816b-eab8-4949-8b8"
BQ_DATASET = "fraud_features"
KAFKA_BROKER = "kafka1:9092"  # Hoặc IP máy ảo nếu chạy khác mạng
TOPIC_NAME = "raw_transactions"

# 1. Khai báo Schema định dạng JSON mà Data Generator bắn vào Kafka
kafka_schema = StructType([
    StructField("trans_date_trans_time", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("gender", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("unix_time", LongType(), True)
])

def process_micro_batch(batch_df, batch_id):
    """
    Hàm này sẽ được gọi tự động mỗi 2 phút.
    Nó nhận vào 1 Dataframe chứa dữ liệu của 2 phút vừa qua.
    """
    # Nếu batch không có dữ liệu thì bỏ qua
    if batch_df.isEmpty():
        print(f"[*] Batch {batch_id} trống. Bỏ qua.")
        return

    print(f"\n{'='*40}")
    print(f"[*] BẮT ĐẦU XỬ LÝ BATCH ID: {batch_id}")
    
    # Cố định dữ liệu trên RAM để tính toán nhiều bảng không bị đọc lại từ Kafka
    batch_df.persist()
    
    # Đóng dấu thời gian xử lý để Grafana biết đây là mẻ dữ liệu mới nhất
    batch_df = batch_df.withColumn("processing_time", current_timestamp())

    # --- TÍNH TOÁN CÁC BẢNG THỐNG KÊ (AGGREGATIONS) ---

    # --- TÍNH TOÁN CÁC BẢNG THỐNG KÊ (AGGREGATIONS) ---
    # Đã loại bỏ hoàn toàn yếu tố "is_fraud" để giả lập dữ liệu thực tế tinh khiết

    # 1. Thống kê theo Giới tính (Gender)
    # Phục vụ cho Grafana vẽ biểu đồ tròn (Pie Chart) tỉ lệ Nam/Nữ
    gender_summary = batch_df.groupBy("gender", "processing_time").agg(
        count("*").alias("total_transactions"),
        spark_sum("amt").alias("total_amount"),
        avg("amt").alias("avg_amount")
    )

    # 2. Thống kê theo Danh mục Cửa hàng (Category)
    # Phục vụ cho Grafana vẽ biểu đồ cột (Bar Chart) xem danh mục nào chi tiêu nhiều nhất
    category_summary = batch_df.groupBy("category", "processing_time").agg(
        count("*").alias("total_transactions"),
        spark_sum("amt").alias("total_amount"),
        avg("amt").alias("avg_amount")
    )

    # 3. Thống kê theo Vị trí Địa lý (State/City)
    # Phục vụ cho Grafana vẽ bản đồ nhiệt (Geomap) khu vực có lượng giao dịch sôi động
    geo_summary = batch_df.groupBy("state", "city", "processing_time").agg(
        count("*").alias("total_transactions"),
        spark_sum("amt").alias("total_amount"),
        avg("amt").alias("avg_amount")
    )

    # 4. (MỚI) Bảng thống kê Tổng quan (Overall Summary)
    # Đây là bảng cực kỳ quan trọng để em vẽ 2 cái đồng hồ đo (Gauge) to đùng trên cùng Dashboard:
    # "Tổng số giao dịch" và "Tổng dòng tiền chảy qua hệ thống"
    overall_summary = batch_df.groupBy("processing_time").agg(
        count("*").alias("total_transactions_in_batch"),
        spark_sum("amt").alias("total_volume_in_batch"),
        avg("amt").alias("avg_transaction_size")
    )

    # --- GHI TRỰC TIẾP XUỐNG BIGQUERY (KHÔNG QUA GCS) ---
    try:
        # Hàm hỗ trợ ghi nhanh gọn
        def write_to_bq(df, table_name):
            df.write.format("bigquery") \
                .option("table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}") \
                .option("parentProject", GCP_PROJECT_ID) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()

        write_to_bq(gender_summary, "gender_summary")
        write_to_bq(category_summary, "category_summary")
        write_to_bq(geo_summary, "geo_summary")
        write_to_bq(overall_summary, "overall_summary")
        print(f"[+] Đã đẩy thành công Batch {batch_id} lên Google BigQuery!")
        
    except Exception as e:
        print(f"[!] Lỗi khi ghi BigQuery tại Batch {batch_id}: {e}")

    # Giải phóng RAM sau khi ghi xong
    batch_df.unpersist()
    print(f"{'='*40}\n")


def main():
    # 2. Khởi tạo Spark Session với thư viện Kafka và BigQuery
    spark = SparkSession.builder \
        .appName("KafkaToBigQueryPipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("[*] Đang kết nối tới Kafka...")

    # 3. Kết nối trực tiếp vào Kafka Topic
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 4. Ép kiểu dữ liệu từ chuỗi Byte của Kafka sang JSON DataFrame
    parsed_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .withColumn("data", from_json(col("json_string"), kafka_schema)) \
        .select("data.*")

    # 5. Khởi động luồng xử lý: Cứ 2 phút kích hoạt (Trigger) 1 lần
    print("[*] Bắt đầu lắng nghe dữ liệu. Trigger 2 phút/lần...")
    
    query = parsed_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime="2 minutes") \
        .start()

    # Giữ luồng chạy liên tục cho đến khi tắt chương trình
    query.awaitTermination()

if __name__ == "__main__":
    # Đảm bảo môi trường đã nhận file cấu hình GCP
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("[!] CẢNH BÁO: Chưa set biến môi trường GOOGLE_APPLICATION_CREDENTIALS")
        
    main()