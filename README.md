# **Healthcare Revenue Cycle Management (RCM) Azure Data Engineering Project: Medallion Architecture**

This project is a data engineering solution built on Microsoft Azure, focused on processing car sales data using the Medallion Architecture (Raw, Curated, and Refined layers). It automates data workflows to provide scalable and efficient processing from raw data to analytics-ready insights.

## **System Architecture Overview**

The Medallion Architecture guides the data transformation, where the raw data flows through the following stages:

1. **Raw Layer** - Raw data
2. **Curated Layer** - Cleaned and enriched data
3. **Refined Layer** - Analytics-ready data

![image](https://github.com/user-attachments/assets/9842d4b4-3ef2-4522-98e4-2e866a467efc)

---

## **Project Overview**

This project implements a robust data pipeline for car sales data using the Medallion framework. By leveraging Azure tools, the pipeline is designed to handle large-scale data processing and provide structured insights at the end.

### **Key Features**

- **Three-Tier Architecture:** Data is processed and stored in separate stages (Raw, Curated, Refined) for easy management and optimization.
- **Automated Pipeline:** Azure Data Factory orchestrates the entire pipeline, moving and transforming data automatically.
- **Incremental Loading:** Data is added incrementally to the Raw layer, ensuring efficient processing.
- **Data Cleanup:** Databricks handles data transformations, making the final dataset ready for analytics.
- **Star Schema & SCD Type-1:** A star schema is used to structure the final dataset, with Slowly Changing Dimension (SCD) Type-1 for updating historical data.

---

## **Technologies Used**

- **Azure Data Factory** for orchestrating data movement.
- **Azure SQL Database** for storing raw data.
- **Azure Data Lake Gen 2** for tiered storage (Raw, Curated, Refined).
- **Azure Databricks** for data transformation and enrichment.
- **Star Schema & SCD Type-1** for efficient data modeling.

---

## **Project Workflow**

### **Step 1: Raw Data Ingestion**
Data is fetched from an external source (e.g., GitHub) and loaded into Azure SQL Database using Azure Data Factory.

### **Step 2: Incremental Data Loading**
New data is loaded incrementally into the Raw layer of Azure Data Lake Gen 2. Azure Data Factory automates this process.

### **Step 3: Data Transformation**
Databricks processes the data, cleans it, and applies necessary transformations to build the Curated and Refined layers, structured into a star schema.

### **Step 4: Reporting-Ready Data**
The Refined layer is the final output—cleaned and enriched data that can be used for business intelligence or reporting.

---

## **Pipeline Details**

### **Pipeline 1: Data Ingestion**

This pipeline fetches data from GitHub and loads it into the Azure SQL Database.

![image](https://github.com/user-attachments/assets/69ea4d58-c5d7-4361-b1e1-4b0ed59f07ae)

### **Pipeline 2: Incremental Data Loading**

This pipeline ensures that only new data is appended to the Raw layer in Azure Data Lake Gen 2.

![image](https://github.com/user-attachments/assets/6f1d9cc0-c880-4a67-916f-330e43bda20b)

The expression in the pipeline's expression builder dynamically filters data from source_cars_data based on incremental loading. It selects records where Date_ID falls between the last and current load values, ensuring only new data is processed. A screenshot is provided for better clarity.

![image](https://github.com/user-attachments/assets/a3338793-d369-416c-9dfe-aa2d8ddc9be9)

The stored procedure uses the below expression to retrieve the maximum date from the current load. This ensures that only new records up to the latest available date are processed in the incremental data pipeline.

![image](https://github.com/user-attachments/assets/fc0ad4df-75b1-478c-bdff-9878b9e2b998)

### **SQL Procedures and Data Transformation**

SQL tables and procedures are created to facilitate data cleaning and transformations.

![image](https://github.com/user-attachments/assets/6154a51b-2b6a-4132-981e-8691dff1c70e)

### **Databricks Workflow: Building the Star Schema**

The raw data is transformed into structured tables with a star schema in Databricks.

![image](https://github.com/user-attachments/assets/d9b4f062-75ae-40f7-b8bc-81d39efdef7d)

### **Updated Incremental Data Loading Pipeline**

To enhance the efficiency of data transformation, a Databricks notebook has been integrated with the existing Azure Data Factory (ADF) pipeline. This integration allows ADF to orchestrate the entire data processing workflow seamlessly.

- The Databricks notebook is triggered at the end of the incremental loading process in ADF.
- Once the data is ingested into the Raw layer of Azure Data Lake Gen 2, the Databricks notebook processes and moves it to the Curated and Refined layers.
- This approach ensures that both ADF and Databricks function as a single end-to-end pipeline, automating the entire data ingestion and transformation flow.
- The notebook handles data cleansing, transformation, and structuring, making the final dataset ready for analytical use

![image](https://github.com/user-attachments/assets/fb772b49-3815-429b-aa83-3ba6ba49ab3c)

### **Azure Resource Group**

Overview of the deployed resources within Azure for this project.

![image](https://github.com/user-attachments/assets/9f64d999-b3df-4750-b33a-36cc11195461)

---

## **How to Deploy and Run the Project**

Follow these steps to deploy and run the project in your Azure environment.

### **Prerequisites**
- **Azure Subscription** for access to Azure services (Data Factory, Databricks, Data Lake).
- **Basic knowledge of Azure Data Factory** for pipeline creation.
- **Azure Databricks Workspace** setup and cluster configuration.
- **GitHub Repository Access** for fetching the raw data.
- **Azure SQL Database** for storing raw data.

### **Step-by-Step Instructions**

1. **Configure Data Factory Pipelines:**
   - Pipeline 1: Set up data ingestion from GitHub to SQL Database.
   - Pipeline 2: Automate incremental data loading into the Bronze layer of Data Lake.

2. **Set Up Databricks:**
   - Create a Databricks workspace and set up the cluster.
   - Import Databricks notebooks that handle data transformation and schema creation.

3. **Verify Data Flow:**
   - Ensure raw data is stored in SQL Database.
   - Verify that data flows correctly through the Bronze, Silver, and Gold layers in Data Lake.

4. **Star Schema Construction:**
   - Run Databricks jobs to generate the star schema for reporting, implementing SCD Type-1 for historical updates.

5. **Optional Customizations:**
   - Modify workflows and notebooks as needed, including adding custom monitoring or error handling mechanisms.

---
