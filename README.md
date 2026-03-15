# 🚌 ETL Dataset Trans Jakarta

## 🏗️ Data Pipeline Architecture
![ETL Technical Architecture](https://raw.githubusercontent.com/mhdalfarisy/mhdalfarisy.github.io/main/src/assets/images/Diagram_ETL_Image.png)

## 📌 Project Overview
Proyek ini membangun jalur data (**data pipeline**) *End-to-End* yang mengotomatisasi pengolahan data transaksi Trans Jakarta. Alur ini mencakup pengambilan data mentah, pembersihan data secara mendalam dengan Python, penyimpanan di Google BigQuery sebagai Data Warehouse, hingga visualisasi *real-time* di Power BI.

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
Mengimpor dataset transaksi Trans Jakarta dalam format **CSV** sebagai sumber data utama.

### 2. Transformation
Proses ini dilakukan menggunakan skrip Python untuk memastikan kualitas data:
* **Pembersihan Data:** Menangani *null values* dan menghapus data duplikat.
* **Time-Series Ready:** Konversi tipe data kolom tanggal dan waktu untuk kebutuhan analisis tren.
* **Normalisasi:** Standarisasi nama halte dan rute agar konsisten.
* **Optimasi:** Penghitungan metrik dasar untuk mempercepat performa *query* di Data Warehouse.

### 3. Loading
Mengunggah hasil transformasi secara efisien ke tabel di **Google BigQuery** menggunakan *service account* yang aman (`.json` key).

### 4. Logging & Monitoring
Sistem mencatat setiap eksekusi ke dalam **Log File** dan **Log CSV** untuk memantau keberhasilan proses secara otomatis.

---

## 🚀 Key Features in Script
Skrip `ETL_Trans_Jakarta.py` dirancang dengan standar profesional:
* **Error Handling:** Menjaga stabilitas skrip agar tidak berhenti saat menemui anomali data.
* **Automated Logging:** Melacak status eksekusi (Success/Fail) secara *real-time*.
* **BigQuery Auto-Schema:** Memastikan struktur tabel di cloud tetap sinkron dengan data lokal secara otomatis.

---

## 📂 File Structure
```text
.
├── src/
│   └── ETL_Trans_Jakarta.py      # Script utama ETL
├── logs/
│   ├── log_file.txt              # Riwayat eksekusi (text)
│   └── log_summary.csv           # Ringkasan eksekusi (tabular)
├── assets/
│   └── Diagram_ETL_Image.png     # Arsitektur teknis
└── README.md
