import threading

import pandas as pd
import pandas.tseries.offsets as offsets
import pymysql
import requests
from elasticsearch import Elasticsearch
from read_db_credentials import *

# 此檔案用來將sql資料丟入elastic

doc_cnt = 0


def connect_sql():
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            passwd=mysql_pswd,
            db='PMart',
            port=mysql_port,
            charset='utf8mb4')
    except (AttributeError, pymysql.OperationalError):
        conn = connect_sql()
    return conn


def connect_es():
    es_url = es_host
    conn = Elasticsearch(es_url)
    # print(requests.get(es_url), ' ======>    ES server Connected')
    return conn


def get_max_date(conn):
    sql = 'SELECT MAX(Date) maxDate FROM PMart.transaction_flow_for_ELK;'
    try:
        d = pd.read_sql(sql=sql, con=conn)
        max_d = d["maxDate"][0]
    except (AttributeError, pymysql.OperationalError):
        conn = connect_sql()
        d = pd.read_sql(sql=sql, con=conn)
        max_d = d["maxDate"][0]
    return max_d, conn


def sending_job(ii, start, cnt, d):
    global doc_cnt, i, max_cnt
    es = connect_es()
    print("Thread" + str(ii) + " start!")
    progress = 0
    added = 0
    freq = 100
    for j in range(0, cnt):
        es.create(index='elk_flow_test4', id=j + start, body=d[j])
        if j % int(cnt / freq) == 0:
            if j != 0:
                print("Thread" + str(ii) + ": " + str(progress) + "%")
                doc_cnt = doc_cnt + j + 1 - added
                added = j + 1
                print("Total progress: {0:.3%}".format(doc_cnt / max_cnt))
            progress += 100 / freq
    doc_cnt = doc_cnt + j + 1 - added
    print("Total progress: {0:.3%}".format(doc_cnt / max_cnt))
    es.transport.close()
    print("Thread" + str(ii) + " done!")
    # print(str(cnt) + " documents sent")
    # print(str(doc_cnt) + " documents in total sent")
    if i > 1:
        i = i - 1
        print(str(i) + " threads left")
    else:
        print("All threads done!")


if __name__ == "__main__":
    db = connect_sql()
    sql_str_data = 'SELECT MIN(Date) minDate FROM PMart.transaction_flow_for_ELK;'
    df = pd.read_sql(sql=sql_str_data, con=db)
    min_date = df["minDate"][0]
    print("minDate:", end=" ")
    print(min_date)

    max_date, db = get_max_date(db)
    print("maxDate:", end=" ")
    print(max_date)

    cursor_date = min_date
    print("cursorDate:", end=" ")
    print(cursor_date)

    sql_str_data = 'SELECT COUNT(*) max_cnt FROM PMart.transaction_flow_for_ELK;'
    df = pd.read_sql(sql=sql_str_data, con=db)
    max_cnt = int(df["max_cnt"][0])
    i = 0
    interval = 20
    counts = 0

    while cursor_date <= max_date:
        sql_str_data = 'SELECT * FROM PMart.transaction_flow_for_ELK WHERE Date >= \"' + str(cursor_date) \
                       + '\" AND Date < DATE_ADD(\"' + str(cursor_date) + '\", INTERVAL ' + str(interval) + ' DAY);'

        df = pd.read_sql(sql=sql_str_data, con=db)
        doc = df.to_dict(orient="records")
        row_count = df.shape[0]

        cursor_date = cursor_date + offsets.Day(interval)

        job = threading.Thread(target=sending_job, args=(i, counts, row_count, doc))

        job.start()
        counts = counts + row_count
        i += 1
