🏨 Hotel Booking Analytics

Python ETL | Data Lake | Data Warehouse | Data Analysis

📌 Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using Python on a real-world Hotel Booking Demand dataset (119,000+ records).

The project focuses on analyzing hotel booking behavior, seasonal trends, cancellation patterns, and pricing insights, while following Data Lake and Data Warehouse concepts commonly used in analytics systems.

🏗️ Architecture Overview
Raw Data (Data Lake)
        ↓
   Python ETL
        ↓
Cleaned Data (Data Warehouse)
        ↓
 Analytics & Visualizations

📂 Project Structure
Tourist-Analytics/
│
├── data/
│   ├── raw/
│   │   └── hotel_bookings.csv          # Data Lake (raw data)
│   │
│   └── transformed/
│       └── cleaned_hotel_bookings.csv  # Data Warehouse (clean data)
│
├── scripts/
│   └── etl.py                           # ETL pipeline script
│
├── visuals/
│   ├── top_hotels.png
│   ├── monthly_bookings.png
│   ├── cancellation_rate.png
│   └── adr_by_hotel.png
│
├── README.md

📊 Dataset Information

Source: Kaggle – Hotel Booking Demand Dataset

Records: 119,390 rows

Columns: 32

Domain: Travel / Hospitality

Key features include:

Hotel type (City / Resort)

Booking and arrival dates

Stay duration

Guest counts

Cancellation status

Country

Average Daily Rate (ADR)

🔄 ETL Pipeline
1️⃣ Extract

Reads raw CSV data from data/raw/

Raw data is stored without modification
➡ Acts as a Data Lake

2️⃣ Transform

Handles missing values

Converts date columns

Creates derived features:

total_stay

total_guests

Prepares clean, structured data

3️⃣ Load

Saves transformed data to data/transformed/

Analytics-ready format
➡ Acts as a Data Warehouse

📈 Key Insights
🔹 Booking Trends

City Hotels receive significantly more bookings than Resort Hotels

🔹 Cancellation Analysis

City Hotels have a higher cancellation rate (~41.7%)

Resort Hotels show more stable booking behavior

🔹 Seasonal Trends

Peak booking months occur between July and October

Clear seasonality in hotel demand

🔹 Pricing Insights

City Hotels have higher Average Daily Rates (ADR) compared to Resort Hotels

🛠️ Tools & Technologies

Python

Pandas

Matplotlib

CSV (Data Lake & Data Warehouse simulation)

▶️ How to Run the Project

Clone the repository

git clone <repository-url>
cd Tourist-Analytics


Install dependencies

pip install pandas matplotlib


Add dataset to:

data/raw/hotel_bookings.csv


Run the ETL pipeline

python scripts/etl.py


Outputs:

Cleaned data → data/transformed/

Visualizations → visuals/

🚀 Future Enhancements

Load data into PostgreSQL / MySQL

Store warehouse data in Parquet format

Build Power BI / Tableau dashboard

Automate ETL using Airflow
