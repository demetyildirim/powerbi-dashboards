import requests
import pandas as pd 
import pyodbc
from datetime import datetime

#---SQL Server Connection---
conn_str = (
    "DRIVER={ODBC DRIVER 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=UrbanLife;"
    "Trusted_Connection=yes;"
)

weather_url = "https://api.open-meteo.com/v1/forecast"
air_url = "https://air-quality-api.open-meteo.com/v1/air-quality"

#---cities from DB---
def fetch_cities():
    sql = """
    SELECT city_id, city, country, latitude, longitude
    FROM dbo.dim_city
    WHERE (city = 'Istanbul' AND country = 'TR')
       OR (city = 'Berlin'  AND country = 'DE')
       OR (city = 'Lisbon'  AND country = 'PT')
    """
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql_query(sql, conn)
    return df
    #---weather (daily)---

def fetch_weather_daily(lat, lon, timezone="auto", past_days=7):
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": timezone,
        "past_days": past_days
    }
    r = requests.get(weather_url, params=params, timeout=30)
    r.raise_for_status()
    j= r.json()

    daily =j.get("daily", {})
    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])

    df = pd.DataFrame({
        "date": dates,
        "max_temp": tmax,
        "min_temp": tmin
    })
    df["avg_temp"] = (df["max_temp"] + df["min_temp"])/2.0
    return df

#---air quality (hour)---
def fetch_pm25_daily(lat, lon, timezone="auto", past_days=7):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm2_5",
        "timezone": timezone,
        "past_days": past_days
    }
    r = requests.get(air_url, params=params, timeout=30)
    r.raise_for_status()
    j= r.json()

    hourly = j.get("hourly", {})
    times = hourly.get("time", [])
    pm = hourly.get("pm2_5", [])

    df = pd.DataFrame({
        "time": times,
        "pm2_5": pm
    })

    df["date"] = df["time"].str.slice(0,10)
    pm_daily = df.groupby("date", as_index=False)["pm2_5"].mean()
    pm_daily.rename(columns={"pm2_5": "pm25_avg"}, inplace=True)
    return pm_daily

#---Life Score---
def compute_scores(df):

    df["extreme_flag"] = ((df["max_temp"] > 35) | (df["min_temp"] < -5)).astype(int)
    pm = df["pm25_avg"].fillna(25)
    pm_score = (100 - (pm.clip(lower=0, upper=50) * 2)).clip(lower=0, upper=100)
    temp = df["avg_temp"].fillna(20)
    temp_score = (100 - (temp - 21).abs() * 5).clip(lower=0, upper=100)
    extreme_score = (100 - df["extreme_flag"] * 30).clip(lower=0, upper=100)

    df["life_score"] = (pm_score * 0.5 + temp_score * 0.3 + extreme_score * 0.2).round(2)
    return df

#---Upsert to SQL Server---
def upsert_fact(df_fact):
    merge_sql = """
    MERGE dbo.fact_city_daily AS t
    USING (SELECT ? AS [date], ? AS city_id) AS s
    ON (t.[date] = s.[date] AND t.city_id = s.city_id)
    WHEN MATCHED THEN UPDATE SET
        avg_temp = ?,
        max_temp = ?,
        min_temp = ?,
        pm25_avg = ?,
        extreme_flag = ?,
        life_score = ?,
        updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT
        ([date], city_id, avg_temp, max_temp, min_temp, pm25_avg, extreme_flag, life_score)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()
        for _, r in df_fact.iterrows():
            cur.execute(
                merge_sql,
                r["date"], int(r["city_id"]),
                float(r["avg_temp"]) if pd.notna(r["avg_temp"]) else None,
                float(r["max_temp"]) if pd.notna(r["max_temp"]) else None,
                float(r["min_temp"]) if pd.notna(r["min_temp"]) else None,
                float(r["pm25_avg"]) if pd.notna(r["pm25_avg"]) else None,
                int(r["extreme_flag"]) if pd.notna(r["extreme_flag"]) else 0,
                float(r["life_score"]) if pd.notna(r["life_score"]) else None,
                r["date"], int(r["city_id"]),
                float(r["avg_temp"]) if pd.notna(r["avg_temp"]) else None,
                float(r["max_temp"]) if pd.notna(r["max_temp"]) else None,
                float(r["min_temp"]) if pd.notna(r["min_temp"]) else None,
                float(r["pm25_avg"]) if pd.notna(r["pm25_avg"]) else None,
                int(r["extreme_flag"]) if pd.notna(r["extreme_flag"]) else 0,
                float(r["life_score"]) if pd.notna(r["life_score"]) else None,
            )
        conn.commit()

def main():
    cities = fetch_cities()
    if cities.empty:
        raise RuntimeError("dim_city içinde Istanbul/Berlin/Lisbon bulunamadı.")

    all_rows = []
    for _, c in cities.iterrows():
        w = fetch_weather_daily(c["latitude"], c["longitude"], timezone="auto", past_days=7)
        a = fetch_pm25_daily(c["latitude"], c["longitude"], timezone="auto", past_days=7)

        df = w.merge(a, on="date", how="left")
        df["city_id"] = int(c["city_id"])
        df = compute_scores(df)

        all_rows.append(df[["date","city_id","avg_temp","max_temp","min_temp","pm25_avg","extreme_flag","life_score"]])

    fact = pd.concat(all_rows, ignore_index=True)

    # upsert to DB
    upsert_fact(fact)

    print("✅ Loaded/Upserted rows:", len(fact))
    print(fact.head(10))

if __name__ == "__main__":
    main()
