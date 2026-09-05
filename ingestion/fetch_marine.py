import psycopg
import requests

LAT = 8.75
LON = 78.10

API_URL = (
    f"https://marine-api.open-meteo.com/v1/marine?"
    f"latitude={LAT}&longitude={LON}&"
    f"hourly=wave_height,wave_direction&"
    f"timezone=UTC&forecast_days=1"
)

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "orca",
    "user": "orca_user",
    "password": "orca_password",
}


def ingest():
    print("Fetching marine forecast from Open-Meteo...")
    res = requests.get(API_URL, timeout=10)
    res.raise_for_status()
    payload = res.json()

    times = payload.get("hourly", {}).get("time", [])
    waves = payload.get("hourly", {}).get("wave_height", [])

    print(f"Received {len(times)} records. Inserting into Postgres...")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for valid_time, height in zip(times, waves):
                if height is not None:
                    cur.execute(
                        """
                        INSERT INTO forecasts 
                        (product_id, variable, value, unit, location, forecast_time, valid_time, quality, metadata)
                        VALUES (
                            18, 
                            'wave_height', 
                            %s, 
                            'm', 
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                            NOW(), 
                            %s::timestamptz, 
                            'good', 
                            '{"source": "Open-Meteo"}'::jsonb
                        );
                        """,
                        (height, LON, LAT, valid_time),
                    )
            conn.commit()

    print("Success: Forecast data inserted into database!")


if __name__ == "__main__":
    ingest()