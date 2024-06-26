import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, StructType
import traceback
import ipinfo
import ipaddress

# Configuración de Kafka y Topics
KAFKA_SERVER = '192.168.56.10:9094'
TOPICS = ['zara-view', 'zara-purchase']

view_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("product_section", StringType(), True),
    StructField("response_code", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True)
])

purchase_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("event", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("product_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("product_section", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("response_code", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("shipping_method", StringType(), True),
    StructField("shipping_cost", IntegerType(), True)
])

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Zara Streaming Analytics") \
    .getOrCreate()

# Establecer el nivel de log a ERROR
spark.sparkContext.setLogLevel("ERROR")

# Leer los datos de Kafka
df_zara_view = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPICS[0]) \
    .option("startingOffsets", "earliest") \
    .load()

df_zara_purchase = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPICS[1]) \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir los datos de Kafka a un DataFrame de Spark
df_view = df_zara_view.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), view_schema).alias("data")) \
    .select("data.*")

df_purchase = df_zara_purchase.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), purchase_schema).alias("data")) \
    .select("data.*")

df_view.printSchema()
df_purchase.printSchema()


def is_valid_ip(ip_address):
    try:
        ipaddress.ip_address(ip_address)
        return True
    except ValueError:
        return False


def geolocate_ip(ip_address):
    try:
        if not is_valid_ip(ip_address):
            return "Unknown", "Unknown", "Unknown", "0,0"

        access_token = 'e1fa2947f365c2' # Token de acceso a la API de IPinfo (Límite de 50,000 peticiones/mes)
        handler = ipinfo.getHandler(access_token)
        details = handler.getDetails(ip_address)
        return (
            details.city, details.region, details.country, details.loc
        )
    except Exception as e:
        print(f"Error al geolocalizar la dirección IP: {e}")
        return "Unknown", "Unknown", "Unknown", "0,0"


# Crear el esquema de geolocalización
geolocation_schema = StructType([
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("loc", StringType(), True)
])

# Registrar la función como UDF
geolocate_ip_udf = udf(geolocate_ip, geolocation_schema)


# Definir la función de procesamiento de batches
def process_batch_view(df, epoch_id):
    try:
        print(f"Procesando batch {epoch_id}")

        # Filtrar los registros con código de respuesta 200 (éxito)
        df = df.filter(col("response_code") == 200)

        # Cambio de divisa de (1 USD = 0.92 EUR)
        df = df.withColumn("product_price", spark_round(df["product_price"] * 0.92, 2))

        # Geolocalizar la dirección IP
        df = df.withColumn("geolocation", geolocate_ip_udf(df["ip"]))
        df = df.select(
            "*",
            col("geolocation.city").alias("city"),
            col("geolocation.region").alias("region"),
            col("geolocation.country").alias("country"),
            col("geolocation.loc").alias("loc")
        ).drop("geolocation")

        # Eliminar la columna de response_code, event, ip, ya que no se requieren
        df = df.drop("response_code", "event", "ip")

        # Agrupar por país y categoria
        df = df.groupBy("country", "product_category").count() \
            .withColumnRenamed("count", "total_views")

        # Mostrar el DataFrame resultante
        df.show()

        # Enviar dataframe a hdfs
        df.write \
            .format("parquet") \
            .mode("append") \
            .save("hdfs://cluster-bda:9000/bda/kafka/Zara/view")

    except Exception as e:
        print(f"Error al procesar el batch: {e}")
        print(traceback.format_exc())


def process_batch_purchase(df, epoch_id):
    try:
        print(f"Procesando batch {epoch_id}")

        # Filtrar los registros con código de respuesta 200 (éxito)
        df = df.filter(col("response_code") == 200)

        # Cambio de divisa de (1 USD = 0.92 EUR)
        df = df.withColumn("total_amount", spark_round(df["total_amount"] * 0.92, 2))
        df = df.withColumn("product_price", spark_round(df["product_price"] * 0.92, 2))
        df = df.withColumn("shipping_cost", spark_round(df["shipping_cost"] * 0.92, 2))

        # Geolocalizar la dirección IP
        df = df.withColumn("geolocation", geolocate_ip_udf(df["ip"]))
        df = df.select(
            "*",
            col("geolocation.city").alias("city"),
            col("geolocation.region").alias("region"),
            col("geolocation.country").alias("country"),
            col("geolocation.loc").alias("loc")
        ).drop("geolocation")

        # Calcular el precio total de la compra, con el costo de envío incluido
        df = df.withColumn("total_price", df["total_amount"] + df["shipping_cost"])

        # Eliminar la columna de response_code, event, ip, currency, ya que no se requieren
        df = df.drop("response_code", "event", "ip", "currency")

        # Agrupar por país y categoria
        df = df.groupBy("country", "product_category").count() \
            .withColumnRenamed("count", "total_purchases")

        # Mostrar el DataFrame resultante
        df.show()

        # Enviar dataframe a hdfs
        df.write \
            .format("parquet") \
            .mode("append") \
            .save("hdfs://cluster-bda:9000/bda/kafka/Zara/purchase")

    except Exception as e:
        print(f"Error al procesar el batch: {e}")
        print(traceback.format_exc())


querys = [df_view.writeStream
          .foreachBatch(process_batch_view)
          .outputMode("append")
          .start(), df_purchase.writeStream
          .foreachBatch(process_batch_purchase)
          .outputMode("append")
          .start()]

# Esperar a que las consultas terminen
for query in querys:
    query.awaitTermination()
