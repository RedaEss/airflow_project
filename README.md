# Velib Station Data Analysis Workflow

## Overview

This project focuses on the retrieval, transformation, and storage of data from [Velib Metropole](https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json). It uses **Apache Airflow** as the central orchestrator to automate and manage the data workflow. The processed data is stored in both **Amazon S3** and **PostgreSQL (Supabase)** for further analysis and visualization in a **Streamlit** [application](https://app-projectdemo2-lead.streamlit.app/).

---

## Producers and Consumers

### Producers

1. **Velib API**
   - Provide the raw data on velib stations 
   
2. **Python Scripts**
   - Fetch data from the Vélib API and store it in AWS S3 and Supabase PostgreSQL db.
   
3. **Storing Data in Supabase PostgreSQL**
   - **PostgreSQL Database**: Transformed data is stored in a PostgreSQL database hosted on Supabase.
   

### Consumers

1. **Supabase PostgreSQL**
   - Transformed data is stored in a PostgreSQL database hosted on Supabase.
   

2. **Streamlit App**
   - A Streamlit app to fetch data from the PostgreSQL database and visualize it.
   - The Streamlit app displays maps and tables to show where there is not enough e-bike availability.
   - The Streamlit app can trigger the Airflow DAG to update the data


## ETL Scripts

1. **Apache Airflow**
   - Airflow DAGs to orchestrate the ETL process.
  
2. **Python Scripts**
   

## Workflow Diagram


