# 🚌 ETL Dataset Trans Jakarta

## 🏗️ Data Pipeline Architecture
![ETL Technical Architecture](https://raw.githubusercontent.com/mhdalfarisy/mhdalfarisy.github.io/main/src/assets/images/Diagram_ETL_Image.png)

## 📌 Project Overview
This project builds an **End-to-End data pipeline** that automates the processing of Trans Jakarta transaction datasets. The workflow encompasses raw data extraction, deep data cleaning using Python, storage in Google BigQuery as a Data Warehouse, and real-time visualization via Power BI.

---

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Libraries:** `Pandas`, `google-cloud-bigquery`
* **Cloud Storage:** Google BigQuery (Data Warehouse)
* **Automation:** Windows Task Scheduler / Python Logging
* **Visualization:** Power BI (DirectQuery/Gateway)

---

## ⚙️ ETL Process Details

### 1. Extraction
Imports Trans Jakarta transaction datasets in **CSV** format as the primary data source.

### 2. Transformation
The transformation process is handled via Python scripts to ensure data quality:
* **Data Cleaning:** Handling null values and removing duplicate records.
* **Time-Series Ready:** Converting date and time columns to appropriate data types for trend analysis.
* **Normalization:** Standardizing bus stop names and routes for consistency.
* **Optimization:** Calculating basic metrics to improve query performance in the Data Warehouse.

### 3. Loading
Efficiently uploads transformed data to **Google BigQuery** tables using a secure service account (`.json` key).

### 4. Logging & Monitoring
The system automatically records every execution into a **Log File** and **Log CSV** to monitor process success and performance.

---

## 🚀 Key Features in Script
The `ETL_Trans_Jakarta.py` script is designed with professional standards:
* **Error Handling:** Ensures script stability by preventing crashes during data anomalies.
* **Automated Logging:** Tracks execution status (Success/Fail) in real-time.
* **BigQuery Auto-Schema:** Automatically ensures the cloud table structure remains synchronized with local data.

---

## 📂 File Structure
```text
.
├── src/
│   └── ETL_Trans_Jakarta.py      # Main ETL script
├── logs/
│   ├── log_file.txt              # Execution history (text)
│   └── log_summary.csv           # Execution summary (tabular)
├── assets/
│   └── Diagram_ETL_Image.png     # Technical architecture diagram
└── README.md
