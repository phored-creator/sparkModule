from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Создаем Spark-сессию
spark = SparkSession.builder \
    .appName("ReceiptAnalytics") \
    .getOrCreate()

# 2. Схема чека (как Spark должен читать JSON)
schema = StructType([
    StructField("id", StringType()),
    StructField("check_sum", DoubleType()),
])

# 3. Читаем из Kafka (только валидные чеки)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "valid_receipts") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Парсим JSON
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Считаем статистику
stats = parsed.agg(
    _sum("check_sum").alias("total_revenue"),
    count("id").alias("total_receipts")
)

# 6. Функция записи в PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/test") \
        .option("dbtable", "stats") \
        .option("user", "postgres") \
        .option("password", "твой_пароль") \
        .mode("append") \
        .save()
    print(f"✅ Батч {batch_id} записан в базу")

# 7. Запускаем поток
query = stats.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()