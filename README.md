
---

## 🛠️ Tech Stack

- **Azure Data Lake Storage Gen2** – for storing raw and processed data
- **Azure Data Factory** – for orchestrating the ETL pipeline
- **Azure Databricks (PySpark)** – for transformation and processing (Silver Layer)
- **Azure Synapse Analytics** – for building external tables and views (Gold Layer)
- **Git & GitHub** – for version control

---

## 🔁 ETL Flow Overview

1. **Bronze Layer**:  
   - Raw CSV files are ingested and stored in ADLS Gen2 in the `/bronze/` container.

2. **Silver Layer**:  
   - Using **PySpark in Databricks**, data is cleaned, validated, and stored in `/silver/`.

3. **Gold Layer**:  
   - External tables and views are created in **Azure Synapse** using `OPENROWSET()` to query curated data.
   - Final analytics-ready tables are exposed for BI tools and reporting.

---

## 🚀 How to Run This Project

### 1. Upload Data
- Place raw `.csv` files into your ADLS Gen2 container under `/bronze/`.

### 2. ADF Pipeline
- Import the ADF JSON files via the **"Manage Hub" > ARM Template** in Azure Data Factory.
- Deploy the pipeline to move data from Bronze → Silver.

### 3. Run Silver Layer in Databricks
- Open `silver_layer.py` in a Databricks notebook.
- Connect to your ADLS storage using a linked service.
- Run the transformation to store data in `/silver/`.
- Add the app id and secret value.

### 4. Create Synapse Views (Gold Layer)
- Open Synapse Studio.
- Use the SQL from `gold_views.sql` to create external views on the silver data.
- These views will represent the **Gold Layer**.

---

## 📊 Output Tables (Gold Layer)

| View Name            | Description                      |
|----------------------|----------------------------------|
| `gold.calender`      | Time and date dimension          |
| `gold.customers`     | Curated customer data            |
| `gold.products`      | Product information              |
| `gold.sales`         | Sales transaction records        |
| `gold.returns`       | Product returns data             |
| `gold.subcat`        | Product subcategories            |
| `gold.territories`   | Sales territories info           |

---

## 🧠 Key Learnings

- Medallion architecture ensures **data quality, traceability, and scalability**.
- Integration between ADF, Databricks, Synapse, and ADLS enables seamless data movement and transformation.
- Secure data handling using Linked Services, Service Principals, and RBAC.

---

## 📎 Resources

- [Azure Data Factory Docs](https://learn.microsoft.com/en-us/azure/data-factory/)
- [Azure Synapse Analytics Docs](https://learn.microsoft.com/en-us/azure/synapse-analytics/)
- [Databricks PySpark Guide](https://learn.microsoft.com/en-us/azure/databricks/pyspark/)

---

## 🤝 Contact

**Erteqa Hossain**  
💼 [LinkedIn](https://www.linkedin.com/in/erteqa-hossain/)  


---

