from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow import DAG
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO


default_args = {
    'owner': 'tanmoy',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

def sql_fetching(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    query = "SELECT name, longitude, latitude FROM top_10_cities;"
    records = hook.get_records(query)
    kwargs['ti'].xcom_push(key='city_coords', value=records)

def transform_daily_weather(city, lon, lat, data):
    daily = data.get("daily", {})
    rows = []

    for i, date in enumerate(daily["time"]):
        rows.append({
            "city": city,
            "latitude": lat,            # lat = 3rd column
            "longitude": lon,           # lon = 2nd column
            "date": date,
            "weather_code": daily["weather_code"][i],
            "temperature_2m_max": daily["temperature_2m_max"][i],
            "temperature_2m_min": daily["temperature_2m_min"][i],
            "sunrise": daily["sunrise"][i],
            "sunset": daily["sunset"][i],
            "daylight_duration": daily["daylight_duration"][i],
            "sunshine_duration": daily["sunshine_duration"][i],
            "uv_index_clear_sky_max": daily["uv_index_clear_sky_max"][i],
            "uv_index_max": daily["uv_index_max"][i],
            "rain_sum": daily["rain_sum"][i],
            "showers_sum": daily["showers_sum"][i],
            "snowfall_sum": daily["snowfall_sum"][i],
            "precipitation_sum": daily["precipitation_sum"][i],
            "precipitation_hours": daily["precipitation_hours"][i],
            "precipitation_probability_max": daily["precipitation_probability_max"][i],
            "wind_speed_10m_max": daily["wind_speed_10m_max"][i],
            "wind_gusts_10m_max": daily["wind_gusts_10m_max"][i],
            "wind_direction_10m_dominant": daily["wind_direction_10m_dominant"][i],
            "shortwave_radiation_sum": daily["shortwave_radiation_sum"][i],
            "et0_fao_evapotranspiration": daily["et0_fao_evapotranspiration"][i]
        })

    return rows

def api_fetching(**kwargs):
    ti = kwargs['ti']
    city_coords = ti.xcom_pull(key='city_coords', task_ids='city_task')

    http_hook = HttpHook(http_conn_id='openmeteo_api', method='GET')

    run_date = kwargs["ds"]  # IMPORTANT: backfill date from Airflow scheduler

    all_rows = []

    for city, lon, lat in city_coords:
        endpoint = (
            f"/v1/forecast?latitude={lat}&longitude={lon}"
            "&daily=weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,"
            "daylight_duration,sunshine_duration,uv_index_clear_sky_max,uv_index_max,"
            "rain_sum,showers_sum,snowfall_sum,precipitation_sum,precipitation_hours,"
            "precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max,"
            "wind_direction_10m_dominant,shortwave_radiation_sum,et0_fao_evapotranspiration"
            f"&start_date={run_date}&end_date={run_date}"
        )

        response = http_hook.run(endpoint)
        data = response.json()

        rows = transform_daily_weather(city, lon, lat, data)
        all_rows.extend(rows)

    ti.xcom_push(key='daily_weather', value=all_rows)

def load_weather_data(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(key='daily_weather', task_ids='fetch_weather_data') or []

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # FIXED TABLE (no invalid column names)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather_data (
            id SERIAL PRIMARY KEY,
            city TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            date DATE,
            weather_code INT,
            temperature_2m_max DOUBLE PRECISION,
            temperature_2m_min DOUBLE PRECISION,
            sunrise TIMESTAMP,
            sunset TIMESTAMP,
            daylight_duration DOUBLE PRECISION,
            sunshine_duration DOUBLE PRECISION,
            uv_index_clear_sky_max DOUBLE PRECISION,
            uv_index_max DOUBLE PRECISION,
            rain_sum DOUBLE PRECISION,
            showers_sum DOUBLE PRECISION,
            snowfall_sum DOUBLE PRECISION,
            precipitation_sum DOUBLE PRECISION,
            precipitation_hours DOUBLE PRECISION,
            precipitation_probability_max DOUBLE PRECISION,
            wind_speed_10m_max DOUBLE PRECISION,
            wind_gusts_10m_max DOUBLE PRECISION,
            wind_direction_10m_dominant DOUBLE PRECISION,
            shortwave_radiation_sum DOUBLE PRECISION,
            et0_fao_evapotranspiration DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    insert_query = """
        INSERT INTO daily_weather_data (
            city, latitude, longitude, date,
            weather_code,
            temperature_2m_max,
            temperature_2m_min,
            sunrise,
            sunset,
            daylight_duration,
            sunshine_duration,
            uv_index_clear_sky_max,
            uv_index_max,
            rain_sum,
            showers_sum,
            snowfall_sum,
            precipitation_sum,
            precipitation_hours,
            precipitation_probability_max,
            wind_speed_10m_max,
            wind_gusts_10m_max,
            wind_direction_10m_dominant,
            shortwave_radiation_sum,
            et0_fao_evapotranspiration
        )
        VALUES (
            %(city)s, %(latitude)s, %(longitude)s, %(date)s,
            %(weather_code)s,
            %(temperature_2m_max)s, %(temperature_2m_min)s,
            %(sunrise)s, %(sunset)s,
            %(daylight_duration)s, %(sunshine_duration)s,
            %(uv_index_clear_sky_max)s, %(uv_index_max)s,
            %(rain_sum)s, %(showers_sum)s, %(snowfall_sum)s,
            %(precipitation_sum)s, %(precipitation_hours)s,
            %(precipitation_probability_max)s,
            %(wind_speed_10m_max)s, %(wind_gusts_10m_max)s,
            %(wind_direction_10m_dominant)s,
            %(shortwave_radiation_sum)s,
            %(et0_fao_evapotranspiration)s
        );
    """

    for row in rows:
        cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()

def upload_to_s3(**kwargs):
    ti = kwargs["ti"]
    rows = ti.xcom_pull(key='daily_weather', task_ids='fetch_weather_data') or []

    if not rows:
        print("No weather data to upload to S3.")
        return

    df = pd.DataFrame(rows)

    csv_data = df.to_csv(index=False)

    run_date = kwargs["ds"]
    year = run_date[:4]
    month = run_date[5:7]
    day = run_date[8:10]

    s3_path = f"weather_daily/year={year}/month={month}/day={day}/weather.csv"

    s3_hook = S3Hook(aws_conn_id='aws_default')

    s3_hook.load_string(
        string_data=csv_data,
        bucket_name="weather-elt-testing",
        key=s3_path,
        replace=True     
    )


with DAG(
    dag_id='weather_daily_etl',
    default_args=default_args,
    description='Weather ETL with backfill starting 2024-11-01',
    start_date=datetime(2025, 11, 12),
    schedule="0 19 * * *",
    catchup=True            
) as dag:
    
    task1 = PythonOperator(
        task_id='city_task',
        python_callable=sql_fetching
    )

    task2 = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=api_fetching
    )

    task3 = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data
    )
    task4 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3
    )


    task1 >> task2 >> task3 >> task4
