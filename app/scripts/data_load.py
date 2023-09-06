import pandas as pd
import config as c
import psycopg2

connection = psycopg2.connect(
    host=c.host,
    port=c.port,
    user=c.username,
    password=c.password,
    database=c.database
)

load_datamart_pass = "SELECT * FROM final.passengers_data_mart"

cursor = connection.cursor()
cursor.execute(load_datamart_pass)
data = cursor.fetchall()

datamart = pd.DataFrame(data, columns=['date', 'percentage_0p', 'percentage_1p', 'percentage_2p',
                                       'percentage_3p', 'percentage_4p'])

datamart.to_parquet('final_datamart_pass_percent.parquet', index=False)


load_dm_max_min = "SELECT * FROM final.cost_trip_datamart"

cursor = connection.cursor()
cursor.execute(load_dm_max_min)
data_max_min = cursor.fetchall()

dm_max_min = pd.DataFrame(data_max_min, columns=['date', 'max_cost_trip_0p', 'min_cost_trip_0p', 
                                                 'max_cost_trip_1p', 'min_cost_trip_1p', 
                                                 'max_cost_trip_2p', 'min_cost_trip_2p', 
                                                 'max_cost_trip_3p', 'min_cost_trip_3p', 
                                                 'max_cost_trip_4p_plus', 'min_cost_trip_4p_plus'])

dm_max_min.to_parquet('final_datamart_max_min.parquet', index=False)
print('Витрины данных успешно сохранены в формате parquet')
