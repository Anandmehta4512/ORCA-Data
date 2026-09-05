
import os
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
def run_pipeline():
    print("\n[30-MIN PIPELINE] Fetching latest marine forecast...")
    run_id = None
    try:
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (source_id, status, started_at)
                    VALUES (6, 'running', NOW())
                    RETURNING id;
                """
                )
                run_id = cur.fetchone()[0]
                conn.commit()

                res = requests.get(API_URL, timeout=15)
                res.raise_for_status()
                data = res.json()

                times = data.get("hourly", {}).get("time", [])
                waves = data.get("hourly", {}).get("wave_height", [])
                processed_count = 0

                for valid_time, val in zip(times, waves):
                    if val is not None:
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
                                '{"source": "Open-Meteo", "pipeline": "30m"}'::jsonb
                            )
                            ON CONFLICT (product_id, location, valid_time)
                            DO UPDATE SET 
                                value = EXCLUDED.value,
                                forecast_time = NOW(),
                                retrieved_at = NOW();
                        """,
                            (val, LON, LAT, valid_time),
                        )
                        processed_count += 1

                cur.execute(
                    """
                    UPDATE ingestion_runs 
                    SET status = 'success', finished_at = NOW(), records_fetched = %s
                    WHERE id = %s;
                """,
                    (processed_count, run_id),
                )
                conn.commit()
                print(
                    f"[30-MIN PIPELINE] Done! Synced {processed_count} records cleanly. Run ID: {run_id}"
                )

    except Exception as err:
        print(f"[ERROR] Pipeline run failed: {err}")
        if run_id:
            with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE ingestion_runs 
                        SET status = 'failed', finished_at = NOW(), error_message = %s
                        WHERE id = %s;
                    """,
                        (str(err), run_id),
                    )
                    conn.commit()


if __name__ == "__main__":
    run_pipeline()