# '''
# =================================================
# Milestone 3

# Nama  : Putri joeliya
# Batch : CODA-020-RMT

# Program ini dibuat untuk melakukan automatisasi extract  data dari dataset kaggle ke Airflow.
# dengan data set yang di gunakan yaitu IBM HR analytics attrition dataset
# =================================================
# '''

import kagglehub
import os
from pyspark.sql import SparkSession

DATASET_ROOT_DIR = "/opt/airflow/data"

# Download dataset dari Kaggle
path = kagglehub.dataset_download("pavansubhasht/ibm-hr-analytics-attrition-dataset")
print("Path to dataset files:", path)
if not os.path.exists(DATASET_ROOT_DIR):
    # Buat folder target jika belum ada
    os.makedirs(DATASET_ROOT_DIR)
os.system("cp -r {}/* {}".format(path, DATASET_ROOT_DIR))
print("Path to dataset files:", path)
