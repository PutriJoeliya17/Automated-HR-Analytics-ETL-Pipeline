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

```text
+------------------+      +---------------------+      +-----------------+
|  python_extract  | ---> |  python_transform   | ---> |   python_load   |
+------------------+      +---------------------+      +-----------------+
        |                            |                         |
 Automated Kaggle           Distributed PySpark         MongoDB Atlas
 Ingestion Script           Cleaning & Feature          Cloud Ingestion
                            Engineering Script          Script
