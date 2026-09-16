# Automated Enterprise HR Analytics Data Pipeline (ETL & Data Quality)

An end-to-end automated data engineering pipeline built using **Apache Airflow**, **Apache Spark (PySpark)**, **Great Expectations**, and **MongoDB Atlas Cloud Database**. This project ingests, validates, cleanses, transforms, and loads the **IBM HR Analytics Attrition Dataset** to provide structured, enterprise-grade data ready for downstream analytics and machine learning applications.

---

## 📌 Table of Contents
- [Project Overview](#-project-overview)
- [Architecture & Technology Stack](#-architecture--technology-stack)
- [Pipeline Architecture & Workflow](#-pipeline-architecture--workflow)
- [Exploratory Data Analysis & Transformation Strategy](#-exploratory-data-analysis--transformation-strategy)
- [Data Validation (Great Expectations Suite)](#-data-validation-great-expectations-suite)
- [Database Integration (MongoDB Atlas)](#-database-integration-mongodb-atlas)
- [Repository Structure](#-repository-structure)
- [Airflow DAG Configuration](#-airflow-dag-configuration)
- [Getting Started & Setup](#-getting-started--setup)
- [Author](#-author)

---

## 📌 Project Overview

Employee attrition poses significant challenges for organization stability and HR planning. This project establishes a robust Data Engineering pipeline that automates the extraction of HR raw data from Kaggle, performs distributed processing and cleaning via PySpark, enforces strict data quality checks via Great Expectations, and loads the structured output into a MongoDB NoSQL database cluster.

---

## 🛠️ Architecture & Technology Stack

- **Workflow Orchestration:** Apache Airflow
- **Data Ingestion:** `kagglehub`, Python Standard Libraries
- **Distributed Data Processing:** Apache Spark (PySpark)
- **Data Quality & Validation Framework:** Great Expectations (GX v0.18+)
- **Database & Cloud Storage:** MongoDB Atlas (Document Store)
- **Development & Analysis:** Jupyter Notebook, Pandas

---

## 🔄 Pipeline Architecture & Workflow

The entire pipeline is structured as a Directed Acyclic Graph (DAG) in Apache Airflow with linear task dependencies:

+------------------+      +---------------------+      +-----------------+
|  python_extract  | ---> |  python_transform   | ---> |   python_load   |
+------------------+      +---------------------+      +-----------------+
        |                            |                         |
 Automated Kaggle           Distributed PySpark         MongoDB Atlas
 Ingestion Script           Cleaning & Feature          Cloud Ingestion
                            Engineering Script          Script

### Task Breakdown

1. **`python_extract` (`Extract_m1.py`):**
   - Downloads the raw `ibm-hr-analytics-attrition-dataset` programmatically using `kagglehub`.
   - Stages raw CSV files into Airflow's centralized data directory `/opt/airflow/data`.

2. **`python_transform` (`Transform_m1.py`):**
   - Reads the raw dataset via PySpark distributed DataFrame.
   - Performs record deduplication (`dropDuplicates`).
   - Removes zero-variance/constant columns (`EmployeeCount`, `Over18`, `StandardHours`).
   - **Feature Engineering:** Creates a composite unique identifier (`Employee_ID_Key = EmployeeNumber_Age`).
   - Applies data filtering rules ($18 \le \text{Age} \le 65$).
   - Exports partition CSV output into `/opt/airflow/data/transformed_data`.

3. **`python_load` (`Load_m1.py`):**
   - Ingests the cleaned PySpark partition data.
   - Establishes a connection to MongoDB Atlas Cluster.
   - Converts the dataset to JSON documents and executes batch inserts into the target collection (`load.HR_data`).

---

## 🔍 Exploratory Data Analysis & Transformation Strategy

During initial exploratory analysis, raw data characteristics were analyzed:
- **Total Initial Records:** 1,470 rows $\times$ 35 features.
- **Null & Duplicate Values:** 0 null values, 0 duplicate rows detected.
- **Static Columns Drop:** `EmployeeCount` (constant value `1`), `Over18` (constant value `'Y'`), and `StandardHours` (constant value `80`) contained zero variance and were safely removed to optimize memory storage.
- **Composite Primary Key:** Added `Employee_ID_Key` to guarantee unique row identification across distributed partitions.

---

## 🧪 Data Validation (Great Expectations Suite)

To ensure high data integrity prior to downstream storage, a data validation suite was implemented using **Great Expectations (GX)** inside `P2M3_putri_joeliya_GX.ipynb`.

The following 15 key expectations were defined and validated (100% Success Rate):
1. **`expect_column_values_to_be_unique`**: Enforces uniqueness on `emplyee_ID_key`.
2. **`expect_column_values_to_be_between`**: Validates `Age` falls within working-age limits ($18$ to $65$).
3. **`expect_column_values_to_be_in_set`**: Ensures `Gender` contains only `["Male", "Female"]`.
4. **`expect_column_values_to_be_of_type`**: Validates `MonthlyIncome` schema type is `int64`.
5. **`expect_column_values_to_not_be_null`**: Ensures `Attrition` target feature has zero null entries.
6. **`expect_column_values_to_be_in_set`**: Confirms `Department` is restricted to `["Sales", "Research & Development", "Human Resources"]`.
7. **`expect_column_value_lengths_to_be_between`**: Checks `OverTime` values are valid strings (`"Yes"` / `"No"`).
8. **`expect_column_values_to_be_in_set`**: Validates `BusinessTravel` categories (`["Travel_Rarely", "Travel_Frequently", "Non-Travel"]`).
9. **`expect_column_values_to_be_between`**: Ensures `Education` rating falls within scale $1$ to $5$.
10. **`expect_column_values_to_be_between`**: Ensures `EnvironmentSatisfaction` rating scale is $1$ to $4$.
11. **`expect_column_values_to_be_in_set`**: Confirms `MaritalStatus` categories (`["Single", "Married", "Divorced"]`).
12. **`expect_column_values_to_be_between`**: Validates `JobLevel` scale between $1$ and $5$.
13. **`expect_column_values_to_be_between`**: Validates `WorkLifeBalance` score between $1$ and $4$.
14. **`expect_column_values_to_be_between`**: Checks `DistanceFromHome` contains valid non-negative ranges ($1$ to $100$).
15. **`expect_column_values_to_be_between`**: Validates `TotalWorkingYears` boundary limits ($0$ to $50$).

---

## 🍃 Database Integration (MongoDB Atlas)

- **Database:** `load`
- **Collection:** `HR_data`
- **Driver:** `pymongo` & `MongoClient`
- Cleaned PySpark records are formatted as JSON document structures and loaded into MongoDB Atlas for fast document querying and dashboard consumption.

---

## ⏱️ Airflow DAG Configuration

- **DAG Name:** `P2M3_putri-joeliya_DAG`
- **Schedule Interval:** `10,20,30 9 * * 6` *(Executes every Saturday at 09:10 AM, 09:20 AM, and 09:30 AM UTC)*
- **Start Date:** `2024-11-01`
- **Retries:** `1 retry` on failure with a `5-minute` retry delay.
- **Catchup:** `False`

---

## 🚀 Getting Started & Setup

### 1. Prerequisites
Ensure you have the following installed on your system/environment:
- Python 3.10+
- Apache Airflow 2.x
- Apache Spark 3.x
- MongoDB Atlas Account / Connection URI

### 2. Environment Setup
Clone this repository and install the required dependencies:

git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
cd your-repo-name

pip install pyspark pymongo airflow kagglehub great-expectations pandas

### 3. Execution

1. Copy script files (`P2M3_putri_joeliya_extract.py`, `P2M3_putri_joeliya_transform.py`, `P2M3_putri_joeliya_load.py`) into your Airflow scripts path (`/opt/airflow/scripts/`).
2. Copy `P2M3_putri_joeliya_DAG_graph.py` into your Airflow DAGs directory (`/opt/airflow/dags/`).
3. Start Airflow Webserver and Scheduler:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
