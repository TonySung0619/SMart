import threading
from datetime import date
from datetime import datetime as dt
from datetime import timedelta
from time import time, sleep
import pandas as pd

from MySQL_Redis.mysql_redis import *

today = ""
day_df_list = []
worker_num = 3
task_done = [0 for j in range(0, worker_num)]
idle = [True for k in range(0, worker_num)]


def day_count(start, end, column):
    global day_df_list, worker_num
    date_start_no_clock = start[0:10]
    date_end_no_clock = end[0:10]

    day_difference = (dt.strptime(date_end_no_clock, "%Y-%m-%d") - dt.strptime(date_start_no_clock,
                                                                               "%Y-%m-%d")).days
    lock = threading.Lock()
    date_start_year = int(date_start_no_clock[0:4])
    date_start_month = int(date_start_no_clock[5:7])
    date_start_day = int(date_start_no_clock[8:])

    delta = timedelta(days=1)

    for i in range(0, day_difference + 1):
        day_key = str(date(date_start_year, date_start_month, date_start_day) + (delta * (day_difference - i)))
        sql_str = 'SELECT 交易日期, 詳細交易時間, 會員編號, 購物籃序號, 商品編號, 商品名稱, 商品數量, 商品單價, 總金額 ' \
                  + 'FROM transaction_flow_for_redis ' \
                  + 'WHERE 交易日期 = \'{}\' ORDER BY 交易日期 DESC'.format(day_key)

        assign_task = True
        while assign_task:
            while sum(idle):
                idle_worker = idle.index(True)
                worker = threading.Thread(target=job, args=(idle_worker, day_key, sql_str, column, lock))
                idle[idle_worker] = False
                worker.start()
                assign_task = False
                break

    try:
        print("All " + str(i + 1) + " tasks assigned, waiting...")
    except UnboundLocalError:
        print("Incorrect time interval requested.")

    while sum(idle) != worker_num:
        continue
    # print(len(day_df_list))
    return day_df_list


def job(n, key, sql_str, column, lock):
    global day_df_list, today, task_done
    if key == today:
        redis_data = get_select(key=key, sql_str=sql_str, use_redis=False)
    else:
        redis_data = get_select(key=key, sql_str=sql_str)
    redis_data = pd.DataFrame(list(redis_data), columns=column)
    with lock:
        day_df_list.append(redis_data)
        print(key + " done!")
    task_done[n] = task_done[n] + 1
    idle[n] = True


def trans_by_day(start, end):
    global day_df_list, today
    today = dt.now().strftime("%Y-%m-%d")
    day_df_list = []
    begin = time()
    columns = ['交易日期', '詳細交易時間', '會員編號', '購物籃序號', '商品編號', '商品名稱', '商品數量', '商品單價', '總金額']

    full_df = day_count(start, end, column=columns)
    total_days = len(full_df)

    date_start_clock = dt.strptime(start, "%Y-%m-%d %H:%M")
    date_end_clock = dt.strptime(end, "%Y-%m-%d %H:%M")

    df = pd.DataFrame(columns=columns)

    for day in range(0, total_days):
        df = pd.concat([df, full_df[day]], ignore_index=True)

    df = df.sort_values(by=['詳細交易時間'], inplace=False, ignore_index=True, ascending=False)

    trans_correct = df[(df['詳細交易時間'] >= date_start_clock) & (df['詳細交易時間'] < date_end_clock)]
    trans_correct = trans_correct.reset_index(drop=True)
    string_start = str(date_start_clock).replace('-','').replace(' ','_').replace(':','')[:-2]
    string_end = str(date_end_clock).replace('-','').replace(' ','_').replace(':','')[:-2]
    trans_correct.to_csv(f'./static/SMart_{string_start}to{string_end}.csv', encoding='utf-8-sig',index=False)
    trans_correct_count = trans_correct.shape[0]
    trans_on_web = trans_correct.head(101)
    csv_name = f'SMart_{string_start}to{string_end}'
    print("Selecting data takes " + str(time() - begin)[:6] + "s.")
    for i in range(0, worker_num):
        print(str(task_done[i]) + " tasks done by worker" + str(i + 1))
    return trans_correct, trans_correct_count, trans_on_web, csv_name


