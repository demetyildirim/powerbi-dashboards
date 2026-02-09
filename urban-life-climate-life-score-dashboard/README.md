# Urban Life – Climate & Life Score Dashboard

This project analyzes urban climate conditions and livability indicators by combining weather data, air quality metrics, and a calculated life score.

The dashboard is built using a simple data engineering pipeline and visualized in Power BI.

---

## 📌 Project Overview

The goal of this project is to:
- Collect daily weather data for selected cities
- Store the data in a SQL Server database
- Calculate a composite **Life Score**
- Visualize climate trends and livability insights in Power BI

Cities included:
- Berlin
- Istanbul
- Lisbon

---

## 🏗️ Architecture

**Data Flow:**

1. Weather data fetched from Open-Meteo API
2. Python ETL process stores data in SQL Server
3. Power BI connects directly to SQL Server
4. Dashboard visualizes climate trends and life score metrics

---

## 🛠️ Tech Stack

- Python (requests, pandas, pyodbc)
- SQL Server
- Power BI
- Open-Meteo API

---

## 📊 Dashboard Highlights

- Today's Average Temperature (°C)
- 14-day Max / Min Temperature Trends
- Life Score Trend by City
- Latest Average Temperature Comparison
- PM2.5 Air Quality Comparison

---

## 🧮 Life Score Logic

Life Score is a composite metric calculated using:
- Average temperature
- Temperature stability
- PM2.5 air quality levels

The score is normalized on a 0–100 scale to represent urban livability.

---

## 📷 Dashboard Preview

<img width="620" height="797" alt="UrbanLifeDashboard" src="https://github.com/user-attachments/assets/94192233-bc85-4ed8-8a76-9649abad68eb" />


---

## 🚀 How to Run

1. Run the ETL script:
```bash
python etl/weather_etl.py
