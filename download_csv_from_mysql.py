import pymysql
import pandas as pd
from read_db_credentials import *

# 注意改db, sql_str_columns, sql_str_data, df.to_csv

db = pymysql.connect(
    host=mysql_host,
    user=mysql_user,
    passwd=mysql_pswd,
    db='PData',
    port=mysql_port,
    charset='utf8mb4')

cursor = db.cursor()  # 建立游標
try:
    sql_str_data = 'SELECT * FROM transaction_data'
    df = pd.read_sql(sql=sql_str_data, con=db)

except Exception as e:
    print(e)

print(df)
df.to_csv('product.csv',encoding='utf-8-sig', index=False)