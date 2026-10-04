import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

load_dotenv()

# ==========================================================
# CONFIGURATION
# ==========================================================

KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

SALES_TOPIC = os.getenv(
    "KAFKA_SALES_TOPIC",
    "sales-events",
)

FINANCE_TOPIC = os.getenv(
    "KAFKA_FINANCE_TOPIC",
    "finance-events",
)

OPERATIONS_TOPIC = os.getenv(
    "KAFKA_OPERATIONS_TOPIC",
    "operations-events",
)

SALES_CHECKPOINT = os.getenv(
    "SPARK_SALES_CHECKPOINT",
    "/tmp/datamesh-spark-sales",
)

FINANCE_CHECKPOINT = os.getenv(
    "SPARK_FINANCE_CHECKPOINT",
    "/tmp/datamesh-spark-finance",
)

OPERATIONS_CHECKPOINT = os.getenv(
    "SPARK_OPERATIONS_CHECKPOINT",
    "/tmp/datamesh-spark-operations",
)

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "datamesh_db")
PG_USER = os.getenv("POSTGRES_USER", "datamesh_user")
PG_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "datamesh_password",
)

# ==========================================================
# SPARK
# ==========================================================

spark = (
    SparkSession.builder
    .appName("DataMeshCoOperateAnalytics")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("DATA MESH - SPARK STRUCTURED STREAMING")
print("=" * 70)
print(f"Kafka server : {KAFKA_SERVER}")
print(f"Sales topic  : {SALES_TOPIC}")
print(f"Finance topic: {FINANCE_TOPIC}")
print(f"Operations   : {OPERATIONS_TOPIC}")
print(f"Spark version: {spark.version}")
print()

# ==========================================================
# SCHEMAS
# ==========================================================

sales_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", StringType(), True),
])

finance_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("event_time", StringType(), True),
])

operations_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField(
        "processing_time_minutes",
        DoubleType(),
        True,
    ),
    StructField("event_time", StringType(), True),
])

# ==========================================================
# POSTGRES CONNECTION
# ==========================================================

def get_connection():
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


# ==========================================================
# KAFKA STREAM READER
# ==========================================================

def read_topic(topic, schema):
    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_SERVER,
        )
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(
            from_json(
                col("value").cast("string"),
                schema,
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            to_timestamp("event_time"),
        )
    )


# ==========================================================
# SALES STREAM
# ==========================================================

sales = (
    read_topic(SALES_TOPIC, sales_schema)
    .filter(col("event_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(
        col("quantity").isNotNull()
        & (col("quantity") > 0)
    )
    .filter(
        col("amount").isNotNull()
        & (col("amount") >= 0)
    )
    .filter(col("region").isNotNull())
    .filter(col("event_time").isNotNull())
)


# ==========================================================
# FINANCE STREAM
# ==========================================================

finance = (
    read_topic(FINANCE_TOPIC, finance_schema)
    .filter(col("event_id").isNotNull())
    .filter(col("account_id").isNotNull())
    .filter(col("transaction_type").isNotNull())
    .filter(
        col("amount").isNotNull()
        & (col("amount") >= 0)
    )
    .filter(col("currency").isNotNull())
    .filter(col("event_time").isNotNull())
)


# ==========================================================
# OPERATIONS STREAM
# ==========================================================

operations = (
    read_topic(
        OPERATIONS_TOPIC,
        operations_schema,
    )
    .filter(col("event_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("warehouse_id").isNotNull())
    .filter(col("status").isNotNull())
    .filter(
        col("processing_time_minutes").isNotNull()
        & (
            col("processing_time_minutes")
            >= 0
        )
    )
    .filter(col("event_time").isNotNull())
)


# ==========================================================
# WRITE SALES
# ==========================================================

def write_sales(batch_df, batch_id):

    rows = batch_df.collect()

    if not rows:
        print(f"[SALES] Batch {batch_id}: no rows.")
        return

    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO sales_data_product (
            event_id,
            customer_id,
            product_id,
            quantity,
            amount,
            region,
            event_time
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO NOTHING
    """

    cur.executemany(
        insert_sql,
        [
            (
                r["event_id"],
                r["customer_id"],
                r["product_id"],
                r["quantity"],
                r["amount"],
                r["region"],
                r["event_time"],
            )
            for r in rows
        ],
    )

    metric_time = datetime.now(timezone.utc).replace(
        second=0,
        microsecond=0,
    )

    metric_sql = """
        INSERT INTO datamesh_metrics (
            metric_time,
            domain,
            metric_name,
            metric_value
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (
            metric_time,
            domain,
            metric_name
        )
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value
    """

    revenue = sum(
        r["amount"] or 0
        for r in rows
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "sales",
            "event_count",
            float(len(rows)),
        ),
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "sales",
            "revenue",
            float(revenue),
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"[SALES] Batch {batch_id}: "
        f"{len(rows)} rows written."
    )

# ==========================================================
# WRITE FINANCE
# ==========================================================

def write_finance(batch_df, batch_id):

    rows = batch_df.collect()

    if not rows:
        print(
            f"[FINANCE] Batch {batch_id}: no rows."
        )
        return

    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO finance_data_product (
            event_id,
            account_id,
            transaction_type,
            amount,
            currency,
            event_time
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO NOTHING
    """

    cur.executemany(
        insert_sql,
        [
            (
                r["event_id"],
                r["account_id"],
                r["transaction_type"],
                r["amount"],
                r["currency"],
                r["event_time"],
            )
            for r in rows
        ],
    )

    metric_time = datetime.now(timezone.utc).replace(
        second=0,
        microsecond=0,
    )

    metric_sql = """
        INSERT INTO datamesh_metrics (
            metric_time,
            domain,
            metric_name,
            metric_value
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (
            metric_time,
            domain,
            metric_name
        )
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value
    """

    income_count = sum(
        r["transaction_type"] == "INCOME"
        for r in rows
    )

    expense_count = sum(
        r["transaction_type"] == "EXPENSE"
        for r in rows
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "finance",
            "transaction_count",
            float(len(rows)),
        ),
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "finance",
            "income_count",
            float(income_count),
        ),
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "finance",
            "expense_count",
            float(expense_count),
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"[FINANCE] Batch {batch_id}: "
        f"{len(rows)} rows written."
    )

# ==========================================================
# WRITE OPERATIONS
# ==========================================================

def write_operations(batch_df, batch_id):

    rows = batch_df.collect()

    if not rows:
        print(
            f"[OPERATIONS] Batch {batch_id}: no rows."
        )
        return

    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO operations_data_product (
            event_id,
            order_id,
            warehouse_id,
            status,
            processing_time_minutes,
            event_time
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO NOTHING
    """

    cur.executemany(
        insert_sql,
        [
            (
                r["event_id"],
                r["order_id"],
                r["warehouse_id"],
                r["status"],
                r["processing_time_minutes"],
                r["event_time"],
            )
            for r in rows
        ],
    )

    metric_time = datetime.now(timezone.utc).replace(
        second=0,
        microsecond=0,
    )

    metric_sql = """
        INSERT INTO datamesh_metrics (
            metric_time,
            domain,
            metric_name,
            metric_value
        )
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (
            metric_time,
            domain,
            metric_name
        )
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value
    """

    delayed_count = sum(
        r["status"] == "DELAYED"
        for r in rows
    )

    average_processing = (
        sum(
            r["processing_time_minutes"] or 0
            for r in rows
        )
        / len(rows)
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "operations",
            "order_count",
            float(len(rows)),
        ),
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "operations",
            "delayed_count",
            float(delayed_count),
        ),
    )

    cur.execute(
        metric_sql,
        (
            metric_time,
            "operations",
            "average_processing_minutes",
            float(average_processing),
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"[OPERATIONS] Batch {batch_id}: "
        f"{len(rows)} rows written."
    )


# ==========================================================
# START STREAMS
# ==========================================================

sales_query = (
    sales.writeStream
    .outputMode("append")
    .foreachBatch(write_sales)
    .option(
        "checkpointLocation",
        SALES_CHECKPOINT,
    )
    .start()
)

finance_query = (
    finance.writeStream
    .outputMode("append")
    .foreachBatch(write_finance)
    .option(
        "checkpointLocation",
        FINANCE_CHECKPOINT,
    )
    .start()
)

operations_query = (
    operations.writeStream
    .outputMode("append")
    .foreachBatch(write_operations)
    .option(
        "checkpointLocation",
        OPERATIONS_CHECKPOINT,
    )
    .start()
)

print("[SPARK] Sales stream started.")
print("[SPARK] Finance stream started.")
print("[SPARK] Operations stream started.")
print("[SPARK] Writing data products to PostgreSQL...")
print()

# ==========================================================
# KEEP STREAMING
# ==========================================================

try:
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    print("\n[STOP] Spark processor stopped.")

finally:
    sales_query.stop()
    finance_query.stop()
    operations_query.stop()
    spark.stop()
