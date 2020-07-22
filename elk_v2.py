import pymysql
from elasticsearch import Elasticsearch
import requests
import pandas as pd
import pickle
import time
from read_db_credentials import *

def connect_sql():
    db = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        passwd=mysql_pswd,
        db='PMart',
        port=mysql_port,
        charset='utf8mb4')
    return db


def lineNotifyMessage(token, msg):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
    return r.status_code


if __name__ == "__main__":

    token = linenotify_token

    t1 = time.time()
    db = connect_sql()


    with open('pickle_time.txt', 'rb') as ft:
        last_enddate = pickle.load(ft)

    try:
        sql_str_data = 'SELECT * FROM PMart.transaction_flow_for_ELK WHERE Date > \'{}\' ORDER BY Date ASC'.format(last_enddate)
        df = pd.read_sql(sql=sql_str_data, con=db)

    except Exception as err:
        print(err)
        print('sql error!!!!!!')
        message = f'sql error,{err}'
        lineNotifyMessage(token, message)



    row_count = int(df.shape[0])

    run = 0
    while run < 1:
        if row_count == 0:
            print('No new data, stop the process')
            message = 'No new data, stop the process'
            lineNotifyMessage(token, message)
            break
        else:
            print(f'row_count:{row_count}')

            enddate = df.iat[row_count-1,7]

            with open('pickle_time.txt', 'wb') as ft:
                pickle.dump(enddate, ft)

            doc = df.to_dict(orient="records")

            with open('pickle_id.txt', 'rb') as fi:
                last_id = int(pickle.load(fi))

            now_id = last_id + row_count


            with open('pickle_id.txt', 'wb') as fi:
                pickle.dump(now_id, fi)


            try:
                url = es_host
                print(requests.get(url), ' ======>    Server Connected')

                es = Elasticsearch(es_host)
                print('ES object : ', es)
                
            except Exception as err:
                print(f'ES_linking error,{err}')
                message = f'ES_linking error,{err}'
                lineNotifyMessage(token, message)

            send_count = 0

            for i in range(0, row_count):
                try:
                    doc[i]['Date'] = doc[i]['Date'].tz_localize('Asia/Taipei')
                    es.create(index='elk_flow_real', id=last_id+1+i, body=doc[i])
                    send_count += 1
                except Exception as err:
                    print(f'ES_sending error,{err}')
                    message = f'ES_sending error,{err}'
                    lineNotifyMessage(token, message)

            t2 = time.time()
            print(f'send_count:{send_count}')
            print(f'taking {t2-t1:.2f} seconds')
            message = f'SQL to ELK task complete! send_count:{send_count}, taking {t2-t1:.2f} seconds'
            lineNotifyMessage(token, message)
            run += 1
