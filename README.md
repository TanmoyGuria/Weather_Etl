# 🌦️ Weather ETL Pipeline  
### **Airflow | PostgreSQL | AWS S3 | Python**

This project implements an end-to-end **ETL pipeline** to collect, process, and store weather data using **Apache Airflow**, **AWS S3**, and **PostgreSQL**. The pipeline automates data extraction from an external API, transforms it into structured format, and loads it into a data warehouse for analytics.

---

## 🚀 **Project Overview**

The pipeline performs the following steps:

1. **Extract:**  
   - Fetch real-time weather data from a public API (OpenWeather API).

2. **Transform:**  
   - Clean, validate, and structure the JSON response.  
   - Convert data into tabular format.  
   - Standardize timestamps, temperature units, and missing values.

3. **Load:**  
   - Store raw data in **AWS S3** (data lake).  
   - Load transformed data into **PostgreSQL** (data warehouse).  

4. **Orchestrate:**  
   - Use **Apache Airflow** to schedule and monitor ETL tasks.

---

## 🛠 **Tech Stack**

- **Apache Airflow** – Workflow orchestration  
- **AWS S3** – Data lake for raw + processed files  
- **PostgreSQL** – Data warehouse  
- **Python** – API calls, transformations  
- **Docker** – Airflow environment  
- **OpenWeather API** – Weather data source  

