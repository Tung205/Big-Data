from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    when,
    to_timestamp,
    datediff,
    lit
)

from utils import (
    load_config,
    download_from_gcs,
    ensure_dir,
    write_profiles_to_redis,
    ensure_bigquery_dataset,
    write_spark_df_to_bigquery
)


def has_columns(df, required_columns):
    return all(c in df.columns for c in required_columns)


def main():
    config = load_config()

    gcs_config = config["gcs"]
    output_config = config["output"]
    redis_config = config.get("redis", {"enabled": False})
    bigquery_config = config.get("bigquery", {"enabled": False})

    output_base = output_config["base_path"]
    ensure_dir(output_base)

    # 1. Download train.csv từ GCS về container
    download_from_gcs(
        bucket_name=gcs_config["bucket_name"],
        source_blob=gcs_config["source_blob"],
        local_path=gcs_config["local_path"]
    )

    # 2. Tạo SparkSession
    spark = SparkSession.builder \
        .appName("FraudBatchFeaturePipeline") \
        .config("spark.driver.memory", "3g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. Đọc train.csv
    df = spark.read.csv(
        gcs_config["local_path"],
        header=True,
        inferSchema=True
    )

    print("===== RAW SCHEMA =====")
    df.printSchema()

    print("===== RAW SAMPLE =====")
    df.show(5, truncate=False)

    # 4. Chuẩn hóa dữ liệu
    # Dataset fraudTrain thường có:
    # trans_date_trans_time, cc_num, merchant, category, amt, first, last, gender,
    # street, city, state, zip, lat, long, city_pop, job, dob, trans_num,
    # unix_time, merch_lat, merch_long, is_fraud

    if "trans_date_trans_time" in df.columns:
        df = df.withColumn("trans_time", to_timestamp(col("trans_date_trans_time")))
    elif "unix_time" in df.columns:
        df = df.withColumn("trans_time", to_timestamp(col("unix_time")))
    else:
        raise ValueError("Không tìm thấy cột thời gian: trans_date_trans_time hoặc unix_time")

    if "amt" not in df.columns:
        raise ValueError("Không tìm thấy cột amt")

    if "cc_num" not in df.columns:
        raise ValueError("Không tìm thấy cột cc_num")

    if "is_fraud" not in df.columns:
        print("Không tìm thấy cột is_fraud, tự tạo is_fraud = 0")
        df = df.withColumn("is_fraud", lit(0))

    df = df.withColumn("amt", col("amt").cast("double")) \
           .withColumn("is_fraud", col("is_fraud").cast("int"))

    df = df.dropna(subset=["cc_num", "amt", "trans_time"])

    print("===== CLEANED SCHEMA =====")
    df.printSchema()

    print("===== CLEANED SAMPLE =====")
    df.show(5, truncate=False)

    # 5. Chọn ngày mới nhất trong dataset làm mốc để tính 1d / 7d / 30d
    # Không dùng current_date(), vì dataset Kaggle thường là dữ liệu lịch sử.
    max_trans_time = df.select(spark_max("trans_time").alias("max_time")).first()["max_time"]

    if max_trans_time is None:
        raise ValueError("Không lấy được max_trans_time từ dataset")

    print(f"Max transaction time in dataset: {max_trans_time}")

    # 6. Feature theo tài khoản/thẻ
    account_features = df.groupBy("cc_num").agg(
        count("*").alias("transaction_count"),
        spark_sum("amt").alias("total_amount"),
        avg("amt").alias("avg_amount"),
        spark_min("amt").alias("min_amount"),
        spark_max("amt").alias("max_amount"),
        spark_sum("is_fraud").alias("fraud_count")
    ).withColumn(
        "fraud_rate",
        when(col("transaction_count") > 0, col("fraud_count") / col("transaction_count"))
        .otherwise(0)
    )

    print("===== ACCOUNT FEATURES =====")
    account_features.show(20, truncate=False)

    # 7. Feature 1 ngày / 7 ngày / 30 ngày theo từng tài khoản
    recent_df = df.withColumn(
        "days_ago",
        datediff(lit(max_trans_time), col("trans_time"))
    )

    recent_features = recent_df.groupBy("cc_num").agg(
        spark_sum(when(col("days_ago") <= 1, col("amt")).otherwise(0)).alias("amount_1d"),
        spark_sum(when(col("days_ago") <= 7, col("amt")).otherwise(0)).alias("amount_7d"),
        spark_sum(when(col("days_ago") <= 30, col("amt")).otherwise(0)).alias("amount_30d"),

        spark_sum(when(col("days_ago") <= 1, 1).otherwise(0)).alias("txn_count_1d"),
        spark_sum(when(col("days_ago") <= 7, 1).otherwise(0)).alias("txn_count_7d"),
        spark_sum(when(col("days_ago") <= 30, 1).otherwise(0)).alias("txn_count_30d")
    )

    print("===== RECENT FEATURES =====")
    recent_features.show(20, truncate=False)

    # 8. Feature theo category
    if "category" in df.columns:
        category_summary = df.groupBy("category").agg(
            count("*").alias("transaction_count"),
            spark_sum("amt").alias("total_amount"),
            avg("amt").alias("avg_amount"),
            spark_min("amt").alias("min_amount"),
            spark_max("amt").alias("max_amount"),
            spark_sum("is_fraud").alias("fraud_count")
        ).withColumn(
            "fraud_rate",
            when(col("transaction_count") > 0, col("fraud_count") / col("transaction_count"))
            .otherwise(0)
        )

        print("===== CATEGORY SUMMARY =====")
        category_summary.show(20, truncate=False)
    else:
        category_summary = None
        print("No category column found. Skip category_summary.")

    # 9. Feature theo vị trí địa lý
    if has_columns(df, ["state", "city"]):
        geo_summary = df.groupBy("state", "city").agg(
            count("*").alias("transaction_count"),
            spark_sum("amt").alias("total_amount"),
            avg("amt").alias("avg_amount"),
            spark_sum("is_fraud").alias("fraud_count")
        ).withColumn(
            "fraud_rate",
            when(col("transaction_count") > 0, col("fraud_count") / col("transaction_count"))
            .otherwise(0)
        )

        print("===== GEO SUMMARY =====")
        geo_summary.show(20, truncate=False)
    else:
        geo_summary = None
        print("No state/city columns found. Skip geo_summary.")

    # 10. Feature theo gender
    if "gender" in df.columns:
        gender_summary = df.groupBy("gender").agg(
            count("*").alias("transaction_count"),
            spark_sum("amt").alias("total_amount"),
            avg("amt").alias("avg_amount"),
            spark_sum("is_fraud").alias("fraud_count")
        ).withColumn(
            "fraud_rate",
            when(col("transaction_count") > 0, col("fraud_count") / col("transaction_count"))
            .otherwise(0)
        )

        print("===== GENDER SUMMARY =====")
        gender_summary.show(20, truncate=False)
    else:
        gender_summary = None
        print("No gender column found. Skip gender_summary.")

    # 11. Feature theo merchant
    if "merchant" in df.columns:
        merchant_summary = df.groupBy("merchant").agg(
            count("*").alias("transaction_count"),
            spark_sum("amt").alias("total_amount"),
            avg("amt").alias("avg_amount"),
            spark_sum("is_fraud").alias("fraud_count")
        ).withColumn(
            "fraud_rate",
            when(col("transaction_count") > 0, col("fraud_count") / col("transaction_count"))
            .otherwise(0)
        )

        print("===== MERCHANT SUMMARY =====")
        merchant_summary.show(20, truncate=False)
    else:
        merchant_summary = None
        print("No merchant column found. Skip merchant_summary.")

    # 12. Ghi output ra Parquet
    # Không dùng toPandas cho raw dataset 343MB.
    print("===== WRITING PARQUET OUTPUTS =====")

    account_features.write.mode("overwrite").parquet(f"{output_base}/account_features")
    recent_features.write.mode("overwrite").parquet(f"{output_base}/recent_features")

    if category_summary is not None:
        category_summary.write.mode("overwrite").parquet(f"{output_base}/category_summary")

    if geo_summary is not None:
        geo_summary.write.mode("overwrite").parquet(f"{output_base}/geo_summary")

    if gender_summary is not None:
        gender_summary.write.mode("overwrite").parquet(f"{output_base}/gender_summary")

    if merchant_summary is not None:
        merchant_summary.write.mode("overwrite").parquet(f"{output_base}/merchant_summary")

    # Clean data lớn: ghi Parquet để sau này load lên GCS hoặc BigQuery.
    df.write.mode("overwrite").parquet(f"{output_base}/clean_transactions")

    print(f"Parquet outputs written to {output_base}")

    # 13. Ghi các bảng aggregate lên BigQuery
    # Chỉ ghi bảng aggregate, không ghi raw 343MB bằng toPandas.
    if bigquery_config.get("enabled", False):
        try:
            project_id = bigquery_config["project_id"]
            dataset_id = bigquery_config["dataset_id"]
            location = bigquery_config.get("location", "US")

            ensure_bigquery_dataset(project_id, dataset_id, location)

            write_spark_df_to_bigquery(
                account_features,
                project_id,
                dataset_id,
                "account_features",
                location
            )

            write_spark_df_to_bigquery(
                recent_features,
                project_id,
                dataset_id,
                "recent_features",
                location
            )

            if category_summary is not None:
                write_spark_df_to_bigquery(
                    category_summary,
                    project_id,
                    dataset_id,
                    "category_summary",
                    location
                )

            if geo_summary is not None:
                write_spark_df_to_bigquery(
                    geo_summary,
                    project_id,
                    dataset_id,
                    "geo_summary",
                    location
                )

            if gender_summary is not None:
                write_spark_df_to_bigquery(
                    gender_summary,
                    project_id,
                    dataset_id,
                    "gender_summary",
                    location
                )

            if merchant_summary is not None:
                write_spark_df_to_bigquery(
                    merchant_summary,
                    project_id,
                    dataset_id,
                    "merchant_summary",
                    location
                )

        except Exception as e:
            print(f"Warning: Cannot write to BigQuery: {e}")
    else:
        print("BigQuery disabled. Skip writing aggregate tables to BigQuery.")

    # 14. Ghi profile vào Redis cho Flink dùng
    if redis_config.get("enabled", False):
        try:
            write_profiles_to_redis(account_features, recent_features, redis_config)
        except Exception as e:
            print(f"Warning: Cannot write to Redis: {e}")
    else:
        print("Redis disabled. Skip writing profiles to Redis.")

    spark.stop()


if __name__ == "__main__":
    main()