# AWS Lakehouse Solution for STEDI

## Purpose

This project demonstrates the implementation of a **Lakehouse Solution** on AWS, leveraging a **Medallion Architecture** approach. The solution processes, sanitizes, and transforms semi-structured data from various sources, including customer records, accelerometer data, and step trainer data. 

### Architecture Overview

The architecture follows a **three-zone approach**:
![image](https://github.com/user-attachments/assets/6c8141f2-129a-4062-a282-42a5d929d2f8)

1. **Landing Zone** – Raw data is ingested into this zone, stored in Amazon S3, and prepared for further processing.
2. **Trusted Zone** – The data is sanitized, filtering records from customers who consented to share their data for research purposes.
3. **Curated Zone** – Clean, enriched data is made available for downstream analytics and machine learning tasks.

---

## Project Structure

### Landing Zone

In the **Landing Zone**, the raw data from the website and mobile app is stored. The tables created here represent raw, unprocessed data.

- **Data Sources**: 
  - **Customer**: Raw customer data from the website.
  - **Accelerometer**: Raw accelerometer readings from the mobile app.
  - **Step Trainer**: Raw step trainer IoT data.

- **SQL Scripts**: 
  - `customer_landing.sql`: Creates a Glue table for customer data.
  - `accelerometer_landing.sql`: Creates a Glue table for accelerometer data.
  - `step_trainer_landing.sql`: Creates a Glue table for step trainer data.

- **Screenshots**: 
  - After running the Glue tables, screenshots of the queried data in **Athena** are provided to show row counts and data preview.

---

### Trusted Zone

In the **Trusted Zone**, data is sanitized to include only records from customers who have agreed to share their data for research purposes.

- **Data Sources**: 
  - **Customer**: Sanitized customer data, including only those who agreed to share their data.
  - **Accelerometer**: Sanitized accelerometer readings from consenting customers.

- **AWS Glue Jobs**:
  - **Customer Data Sanitization Job**: A Glue job that filters customer records based on consent.
  - **Accelerometer Data Sanitization Job**: A Glue job that filters accelerometer readings for consenting customers.

- **Screenshots**: 
  - **Glue Job Visuals**: Visual representation of the Glue jobs in AWS Glue Studio.
  - **SQL Results**: Screenshots of SQL results showing row counts and preview of data in **Athena**.

- **Python Scripts**: 
  - Python scripts generated from AWS Glue jobs for both the customer and accelerometer sanitization processes.

---

### Curated Zone

In the **Curated Zone**, data is cleaned and enriched, combining customer data with accelerometer and step trainer data.

- **Data Sources**: 
  - **Customer**: Cleaned customer data enriched with accelerometer data.
  - **Machine Learning**: Aggregated data from accelerometer and step trainer records, ready for machine learning.

- **AWS Glue Jobs**:
  - **Step Trainer Data Aggregation Job**: Aggregates step trainer data with accelerometer data for customers who agreed to share their data.
  - **Machine Learning Aggregation Job**: Combines step trainer and accelerometer data to create a machine learning-ready dataset.

- **Screenshots**:
  - **Glue Job Visuals**: Visual representation of the Glue jobs in AWS Glue Studio.
  - **SQL Results**: Screenshots of SQL results showing row counts and data preview for both tables (customer_curated, machine_learning_curated).

- **Python Scripts**: 
  - Python scripts generated from AWS Glue jobs for data aggregation and machine learning preparation.

---

## Row Count Verification

After each stage of the pipeline, row counts are verified to ensure correct processing and transformation of data. The expected row counts for each table are as follows:

- **Landing Zone**:
  - Customer: 956 records
  - Accelerometer: 81,273 records
  - Step Trainer: 28,680 records

- **Trusted Zone**:
  - Customer: 482 records
  - Accelerometer: 40,981 records
  - Step Trainer: 14,460 records

- **Curated Zone**:
  - Customer: 482 records
  - Machine Learning: 43,681 records

These row counts are used to verify the integrity and correctness of the transformation process.

---

## Conclusion

This project illustrates how to build a Lakehouse architecture using AWS services such as **AWS Glue**, **S3**, and **Athena**. By implementing the **Medallion Architecture**, we ensure the data is processed efficiently and in a structured manner, ultimately providing clean and enriched datasets for analytics and machine learning.

