import psycopg

print("Testing database connection...")

try:
    conn = psycopg.connect(
        "host=127.0.0.1 port=5432 dbname=orca user=orca_user password=orca_password"
    )

    print("✅ Database connection successful!")

    conn.close()

except Exception as e:
    print("❌ Database connection failed!")
    print(e)