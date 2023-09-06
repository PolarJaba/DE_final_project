import config as c
import psycopg2
import pandas as pd


# Подключение к базе данных
connection = psycopg2.connect(
    host=c.host,
    port=c.port,
    user=c.username,
    password=c.password,
    database=c.database
)

print('Загрузка данных в raw-слой')

# Создание и наполнение таблицы слоя сырых данных
with connection.cursor() as cur:
    query_create_schema = "CREATE SCHEMA IF NOT EXISTS final;"
    cur.execute(query_create_schema)
    query_create = ("CREATE TABLE IF NOT EXISTS final.raw_data (vendorid BIGINT, trip_pickup_datetime TIMESTAMP, "
                    "trip_dropoff_datetime TIMESTAMP, passengers_count INT, trip_distance FLOAT, ratecodeid INT, "
                    "store_and_fwd_flag VARCHAR(8), pulocationid VARCHAR(8), dolocationid VARCHAR(8), payment_type INT,"
                    "fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tools_amount FLOAT, "
                    "improvement_surchange FLOAT, total_amount FLOAT, congestion_surchange FLOAT);")
    cur.execute(query_create)
    cur.execute("COPY final.raw_data FROM '/init_db/data/yellow_tripdata_2020-01.csv' "
                "DELIMITER ',' ENCODING 'UTF8' CSV HEADER;")
    connection.commit()
    cur.close()

print('Создан слой сырых данных')


print('Загрузка данных core-слоя')

# Создание и наполнение таблицы core-слоя
with connection.cursor() as cur:
    query_create = ("CREATE TABLE IF NOT EXISTS final.core_data (trip_dropoff_datetime TIMESTAMP, passengers_count INT, " 
                    "trip_distance FLOAT, fare_amount FLOAT, tip_amount FLOAT, total_amount FLOAT);")
    cur.execute(query_create)
    insert_query = ("INSERT INTO final.core_data (trip_dropoff_datetime, passengers_count, trip_distance, fare_amount, "
                    "tip_amount, total_amount) "
                    "(SELECT trip_dropoff_datetime, passengers_count, trip_distance, "
                    "fare_amount, tip_amount, total_amount "
                    "FROM final.raw_data "
                    "WHERE (trip_distance / EXTRACT(EPOCH FROM (trip_dropoff_datetime - trip_pickup_datetime)))*3600 <= 150 AND "
                    "trip_dropoff_datetime IS NOT NULL AND "
                    "trip_pickup_datetime IS NOT NULL AND "
                    "passengers_count IS NOT NULL AND "
                    "trip_distance > 0 AND "
                    "fare_amount > 0 AND "
                    "total_amount > 0 AND "
                    "EXTRACT(EPOCH FROM (trip_dropoff_datetime - trip_pickup_datetime)) > 0);")
    cur.execute(insert_query)
    connection.commit()
    cur.close()

print('Загрузка завершена')
