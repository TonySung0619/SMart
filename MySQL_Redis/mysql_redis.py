import pymysql
import redis
import datetime
from decimal import Decimal
from read_db_credentials import *


r_pool = redis.ConnectionPool(
    host="localhost",
    port=redis_port,
    password=redis_pswd,
    db=0,
    decode_responses=True)


def connect_redis():
    global r_pool
    try:
        conn = redis.StrictRedis(connection_pool=r_pool, charset="utf-8")
    except:
        conn = connect_redis()
    return conn


global_r = connect_redis()


def redis_get(r, key):
    try:
        # check redis connection, return error if failed connecting 
        r.ping()
        # get value from redis
        value = r.get(key)
        if value:
            # str to list
            value_list = eval(value)
            return value_list
        else:
            return None
    except:
        # reconnect redis
        print("reconnect redis")
        r = connect_redis()
        value = r.get(key)
        if value:
            value_list = eval(value)
            return value_list
        else:
            return None


def connect_sqldb():
    # global global_db, global_cursor
    try:
        # try:
        #     global_cursor.close()
        #     global_db.close()
        # except:
        #     pass
        conn = pymysql.connect(
            host="localhost",
            user='16',
            passwd=mysql_pswd,
            db='PMart',
            port=mysql_port,
            charset='utf8',
            use_unicode=True)
        cursor = conn.cursor()  # 建立游標
        conn.autocommit(True)  # 設定自動確認
    except:
        conn, cursor = connect_sqldb()
    return conn, cursor


global_db, global_cursor = connect_sqldb()


def get_select(key="", sql_str="", use_redis=True):
    value_list = ()
    db, cursor = connect_sqldb()
    r = connect_redis()
    if use_redis and key != "":
        # global global_r
        value_list = redis_get(r, key)
    elif use_redis is False:
        value_list = None
    else:
        print("Specify your redis key...")
        return None

    if value_list is None:  # no existed key in redis, or redis unused
        # print("From MySQL")
        if sql_str == "":
            print("Specify your SQL statement...")
            return None

        # global global_db, global_cursor

        try:
            # check MySQL connection, return error if failed connecting
            db.ping()
            cursor.execute(sql_str)
        except:
            # reconnect MySQL
            print("reconnect MySQL")
            db, cursor = connect_sqldb()
            cursor.execute(sql_str)
        # item[0] means the first column, item[1] means the second column, etc.
        # value_list = [item for item in cursor.fetchall()]
        value_list = cursor.fetchall()
        # or take all columns
        # value_list = [item for item in cursor.fetchall()]
        # return [(column1, column1, ....), (column1, column1, ....), ...]

        # put key & value into redis if value_list != ()
        # if value_list and use_redis:
        if use_redis and value_list != ():
            r.set(key, str(value_list))
    else:
        # print("From Redis")
        pass


    return value_list

