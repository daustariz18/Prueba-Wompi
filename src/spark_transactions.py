import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count
from pyspark.sql import DataFrame

# ===============================
#  CONFIGURACIÓN SPARK / HADOOP
# ===============================

# Configurar HADOOP_HOME y PATH
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

# ================
#  SPARK SESSION
# ================
def create_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.local.dir", "C:/spark-temp")
        .config("spark.hadoop.io.nativeio.load", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_transactions(spark: SparkSession, input_path: str) -> DataFrame:
    """Load the JSONL transactions file using Spark."""
    return spark.read.json(input_path)


def clean_transactions(df: DataFrame) -> DataFrame:
    """Select and transform the relevant columns."""
    return (
        df
        .withColumn("bin", col("payment_method_type.extra.bin").cast("string"))
        .withColumn("date", to_date(col("created_at")))
        .withColumn("amount", col("amount_in_cents").cast("double"))
        .select("bin", "date", "amount")
    )

def aggregate_transactions(df_clean: DataFrame) -> DataFrame:
    """Aggregate transactions by BIN and date."""
    return (
        df_clean.groupBy("bin", "date")
        .agg(
            _count("amount").alias("transaction_count"),
            _sum("amount").alias("total_approved_amount")
        )
    )


def save_parquet(df, output_path: str):
    import pandas as pd
    import os

    # Crear carpeta si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    pdf = df.toPandas()
    pdf.to_parquet(output_path, index=False)


def main():
    INPUT_FILE = r"data\transactions_50k.jsonl"
    OUTPUT_PATH = r"output\resumen_transacciones.parquet"

    spark = create_spark_session("Wompi-DataEngineer-Challenge")

    print("Cargando archivo JSONL...")
    df_raw = load_transactions(spark, INPUT_FILE)

    print("Limpiando datos...")
    df_clean = clean_transactions(df_raw)

    print("Agregando información por BIN y fecha...")
    df_agg = aggregate_transactions(df_clean)

    print("Guardando resultado en Parquet...")
    save_parquet(df_agg, OUTPUT_PATH)

    print(f"Archivo Parquet generado en: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
 
