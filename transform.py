# '''
# =================================================
# Milestone 3

# Nama  : Putri joeliya
# Batch : CODA-020-RMT

# Program ini dibuat untuk melakukan automatisasi Transform  data dari dataset extract ke Airflow.
# dengan data set yang di gunakan yaitu IBM HR analytics attrition dataset
# =================================================
# '''

import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("HR_Attrition_Transform") \
    .getOrCreate()

# Baca data CSV
df = spark.read.csv('/opt/airflow/data/WA_Fn-UseC_-HR-Employee-Attrition.csv', header=True, inferSchema=True)

# Perbaikan: Hapus duplikat lalu hapus kolom dari df_cleaned
df_cleaned = df.dropDuplicates()
cols_to_drop = ["EmployeeCount", "Over18", "StandardHours"]
df_cleaned = df_cleaned.drop(*cols_to_drop)  # <-- Diperbaiki dari df ke df_cleaned

# Tambah kolom dan filter
df_cleaned = df_cleaned.withColumn(
    "Employee_ID_Key", 
    concat_ws("_", col("EmployeeNumber"), col("Age"))
)
df_cleaned = df_cleaned.filter((col("Age") >= 18) & (col("Age") <= 65))

# Simpan ke folder transformed_data
df_cleaned.coalesce(1).write.mode("overwrite").option("header", "true").csv("/opt/airflow/data/transformed_data")
spark.stop()