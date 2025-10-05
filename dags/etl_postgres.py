from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


def load_data(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    data = context['ti'].xcom_pull(task_ids='transform_data')
    rows = [(d["name"], d["value"], d["value_squared"]) for d in data]

    insert_sql = """
    TRUNCATE TABLE target_data;
    INSERT INTO target_data (name, value, value_squared)
    VALUES (%s, %s, %s)
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE target_data;")
            cur.executemany(
                "INSERT INTO target_data (name, value, value_squared) VALUES (%s, %s, %s)",
                rows
            )
        conn.commit()


def load_data():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port=5432
    )
    cur = conn.cursor()
    
    # Crear tabla destino
    cur.execute("""
        CREATE TABLE IF NOT EXISTS target_data (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value DOUBLE PRECISION,
            value_squared DOUBLE PRECISION
        )
    """)
    
    # Crear tabla fuente si no existe y poblarla
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_data (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INT
        )
    """)
    cur.execute("SELECT COUNT(*) FROM source_data")
    if cur.fetchone()[0] == 0:
        cur.executemany(
            "INSERT INTO source_data (name, value) VALUES (%s,%s)",
            [('Alice', 10), ('Bob', 20), ('Charlie', 30)]
        )
    
    # Extraer y transformar
    cur.execute("SELECT id, name, value FROM source_data")
    rows = cur.fetchall()
    transformed = [(r[1], r[2], r[2]**2) for r in rows]
    
    # Cargar en tabla destino
    #cur.execute("TRUNCATE TABLE target_data;")
    cur.executemany(
        "INSERT INTO target_data (name, value, value_squared) VALUES (%s,%s,%s)",
        transformed
    )
    
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    "etl_postgres",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    etl_task = PythonOperator(
        task_id="extract_transform_load",
        python_callable=load_data
    )