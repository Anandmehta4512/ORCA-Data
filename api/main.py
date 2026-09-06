from fastapi import FastAPI, HTTPException, Query
import psycopg
import os
from psycopg.rows import dict_row

app = FastAPI(title="ORCA API")


def get_db():
    return psycopg.connect(
        os.environ["DATABASE_URL"],
        row_factory=dict_row,
    )


@app.get("/")
def home():
    return {"message": "ORCA API is running!", "status": "success"}


@app.get("/sources")
def get_sources():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sources ORDER BY id;")
            return cur.fetchall()


@app.get("/advisories/nearest-pfz")
def get_nearest_pfz(
    lat: float = Query(..., description="Vessel Latitude"),
    lon: float = Query(..., description="Vessel Longitude"),
):
    query = """
    WITH pfz AS (
        SELECT 
            id,
            advisory_text,
            valid_until,
            ST_Distance(
                zone, 
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) / 1000 AS distance_km,
            DEGREES(
                ST_Azimuth(
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    ST_PointOnSurface(zone::geometry)::geography
                )
            ) AS bearing_degrees
        FROM pfz_advisories
    )
    SELECT 
        id,
        advisory_text,
        valid_until,
        ROUND(distance_km::numeric, 2) AS distance_km,
        ROUND(bearing_degrees::numeric, 2) AS bearing_degrees,
        CASE 
            WHEN bearing_degrees >= 337.5 OR bearing_degrees < 22.5 THEN 'North'
            WHEN bearing_degrees < 67.5 THEN 'North-East'
            WHEN bearing_degrees < 112.5 THEN 'East'
            WHEN bearing_degrees < 157.5 THEN 'South-East'
            WHEN bearing_degrees < 202.5 THEN 'South'
            WHEN bearing_degrees < 247.5 THEN 'South-West'
            WHEN bearing_degrees < 292.5 THEN 'West'
            ELSE 'North-West'
        END AS direction
    FROM pfz
    ORDER BY distance_km ASC
    LIMIT 1;
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lon, lat, lon, lat))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="No PFZ data found")
            return row


@app.get("/forecasts/latest")
def get_latest_forecast(
    lat: float = Query(..., description="Vessel Latitude"),
    lon: float = Query(..., description="Vessel Longitude"),
):
    query = """
    SELECT 
        id,
        variable,
        value,
        unit,
        valid_time,
        ROUND((ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000)::numeric, 2) AS distance_km
    FROM forecasts
    ORDER BY location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, valid_time DESC
    LIMIT 6;
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lon, lat, lon, lat))
            return cur.fetchall()


@app.get("/pipeline/history")
def get_pipeline_history():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, started_at, finished_at, status, records_fetched, error_message
                FROM ingestion_runs
                ORDER BY started_at DESC
                LIMIT 10;
            """)
            return cur.fetchall()


@app.get("/ports/nearest")
def get_nearest_port(
    lat: float = Query(..., description="Vessel Latitude"),
    lon: float = Query(..., description="Vessel Longitude"),
):
    query = """
    SELECT 
        id,
        name,
        port_type,
        state,
        ROUND((ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000)::numeric, 2) AS distance_km,
        ROUND(DEGREES(ST_Azimuth(
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            location
        ))::numeric, 2) AS bearing_degrees
    FROM ports
    ORDER BY location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
    LIMIT 1;
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (lon, lat, lon, lat, lon, lat))
            port = cur.fetchone()
            return port if port else {"message": "No port records found nearby"}


@app.get("/vessel/live-advisory")
def get_live_advisory(
    lat: float = Query(..., description="Vessel Latitude"),
    lon: float = Query(..., description="Vessel Longitude"),
):
    with get_db() as conn:
        with conn.cursor() as cur:
            # 1. Latest wave condition
            cur.execute(
                """
                SELECT value AS wave_height, valid_time 
                FROM forecasts 
                WHERE variable = 'wave_height'
                ORDER BY location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, valid_time DESC 
                LIMIT 1;
            """,
                (lon, lat),
            )
            wave = cur.fetchone()

            # 2. Nearest PFZ
            cur.execute(
                """
                SELECT advisory_text, 
                       ROUND((ST_Distance(zone, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1000)::numeric, 2) AS distance_km
                FROM pfz_advisories 
                ORDER BY distance_km ASC 
                LIMIT 1;
            """,
                (lon, lat),
            )
            pfz = cur.fetchone()

            wave_height = wave["wave_height"] if wave else 0.0
            is_safe = wave_height < 2.0

            return {
                "vessel_location": {"latitude": lat, "longitude": lon},
                "sea_condition": {
                    "wave_height_meters": wave_height,
                    "condition": "Calm / Normal"
                    if is_safe
                    else "Rough / High Waves Alert",
                },
                "navigation_advice": "Safe for fishing operations"
                if is_safe
                else "Alert: Return to nearest coastal safe zone immediately",
                "nearest_pfz": pfz,
            }