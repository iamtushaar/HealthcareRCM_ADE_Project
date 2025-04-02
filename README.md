# **Healthcare Revenue Cycle Management (RCM) Azure Data Engineering Project: Medallion Architecture**

This project is a data engineering solution built on Microsoft Azure, focused on processing Healthcare Revenue Cycle Management (RCM) data using the Medallion Architecture (Bronze, Silver, Gold layers). It automates data workflows to provide scalable and efficient processing from raw data to analytics-ready insights.

## **System Architecture Overview**

The Medallion Architecture guides the data transformation, where the raw data flows through the following stages:

1. **Bronze Layer** - Raw data
2. **Silver Layer** - Cleaned and enriched data
3. **Gold Layer** - Analytics-ready data

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/50ddb73fc0196a42e98e2d0d89b25d87917ca7a2/Project%20Screenshots/Architechture%20Dia.png)

---

## **Project Overview**

This project implements a robust data pipeline for Heathcare Revenue Cycle data using the Medallion framework. By leveraging Azure tools, the pipeline is designed to handle large-scale data processing and provide structured insights at the end.

### **Key Features**

- **Three-Tier Architecture:** Data is processed and stored in separate stages (Bronze, Siver, Gold) for easy management and optimization.
- **Automated Pipeline:** Azure Data Factory orchestrates the entire pipeline, moving and transforming data automatically.
- **Incremental Loading:** Data is added incrementally to the Bronze layer, ensuring efficient processing.
- **Data Cleanup:** Databricks handles data transformations, making the final dataset ready for analytics.
- **Snowflake Schema & SCD Type-2:** A star schema is used to structure the final dataset, with Slowly Changing Dimension (SCD) Type-2 for updating historical data.

---

## **Technologies Used**

- **Azure Data Factory** for orchestrating data movement.
- **Azure SQL Database** for storing raw data.
- **Azure Data Lake Gen 2** for tiered storage (Bronze, Siver, Gold).
- **Azure Databricks** for data transformation and enrichment.
- **Azure Key Vault** for storing secrets.
- **Snowflake Schema & SCD Type-2** for efficient data modeling.

---

## **Project Workflow**

### **Step 1: Raw Data Ingestion**
Data is fetched from an external source (e.g., GitHub) and loaded into Azure SQL Database using Azure Data Factory.

### **Step 2: Incremental Data Loading**
New data is loaded incrementally into the Bronze layer of Azure Data Lake Gen 2. Azure Data Factory automates this process.

### **Step 3: Data Transformation**
Databricks processes the data, cleans it, and applies necessary transformations to build the Silver and Gold layers.

### **Step 4: Reporting-Ready Data**
The Gold layer is the final output—cleaned and enriched data that can be used for business intelligence or reporting.

---

## **Project Details**

### **Azure Resource Group**

Overview of the deployed resources within Azure for this project.

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/50ddb73fc0196a42e98e2d0d89b25d87917ca7a2/Project%20Screenshots/Resources.png)


### **Storage Account**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/StorageAccount.png)


### **Linked Services**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/LinkedServices.png)


### **Pipeline 1: HealthcareRCM_EMR_SourcePrep**

This pipeline fetches data from GitHub and loads it into the Azure SQL Databases.

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/d11a204ec3e4c25be11c225993e083d6497f28e5/Project%20Screenshots/HealthcareRCM_EMR_Source_Prep.png)

### **Pipeline 2: HealthcareRCM_EMR_Dataload - Incremental Data Loading**

This pipeline ensures that only new data is appended to the Bronze layer in Azure Data Lake Gen 2.

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/d11a204ec3e4c25be11c225993e083d6497f28e5/Project%20Screenshots/HealthcareRCM_EMR_Dataload1.png)

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/d11a204ec3e4c25be11c225993e083d6497f28e5/Project%20Screenshots/HealthcareRCM_EMR_Dataload2.png)

The expression in the pipeline's expression builder dynamically filters data from source_cars_data based on incremental loading. It selects records where Date_ID falls between the last and current load values, ensuring only new data is processed. A screenshot is provided for better clarity.

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/EMR_Incremental_Data_Ingestion_Logic.png)


### **Pipeline 3: HealthcareRCM_LandingToBronze**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/HealthcareRCM_LandingToBronze.png)


### **Pipeline 4: HealthcareRCM_BronzeToSilver**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/HealthcareRCM_BronzeToSilver1.png)

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/HealthcareRCM_BronzeToSilver2.png)


### **Pipeline 4: HealthcareRCM_SilverToGold**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/HealthcareRCM_SilverToGold.png)

### **Data Model**

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/8bac4e8ff1f013afa7f0e8f2eae5a88e7fae72ec/Project%20Screenshots/Data%20Model.png)


### **MASTER PIPELINE**

To enhance the efficiency of data transformation, a Databricks notebook has been integrated with the existing Azure Data Factory (ADF) pipeline. This integration allows ADF to orchestrate the entire data processing workflow seamlessly.

- The Databricks notebook is triggered at the end of the incremental loading process in ADF.
- Once the data is ingested into the Bronze layer of Azure Data Lake Gen 2, the Databricks notebook processes and moves it to the Silver and Gold layers.
- This approach ensures that both ADF and Databricks function as a single end-to-end pipeline, automating the entire data ingestion and transformation flow.
- The notebook handles data cleansing, transformation, and structuring, making the final dataset ready for analytical use.

![image](https://github.com/iamtushaar/HealthcareRCM_ADE_Project/blob/d11a204ec3e4c25be11c225993e083d6497f28e5/Project%20Screenshots/HealthcareRCM_Master%20Pipeline.png)


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

4. **Schema Construction:**
   - Run Databricks jobs to generate the star schema for reporting, implementing SCD Type-2 for historical updates.

5. **Optional Customizations:**
   - Modify workflows and notebooks as needed, including adding custom monitoring or error handling mechanisms.

---
