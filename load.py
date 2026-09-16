# '''
# =================================================
# Milestone 3

# Nama  : Putri joeliya
# Batch : CODA-020-RMT

# Program ini dibuat untuk melakukan automatisasi Loading  data dari data yang sudah di transform ke Airflow.
# dengan data set yang di gunakan yaitu IBM HR analytics attrition dataset
# =================================================
# '''

import os
from pyspark.sql import SparkSession
from pymongo import MongoClient
import pandas as pd

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("HR_Attrition_Transform") \
    .getOrCreate()
# Baca dari FOLDER hasil
df = spark.read.csv('data/part-00000-ef5f22ef-d8ab-477c-aaa3-4b2801400ca7-c000.csv', header=True,inferSchema=True)

# Koneksi ke MongoDB Atlas & Insert Data
client = MongoClient("mongodb+srv://lukas18:Joeliya17@lukashoch.mvzwgez.mongodb.net/")
HR_analyst = df.toPandas().to_dict(orient='records')
client['load']['HR_data'].insert_many(HR_analyst)