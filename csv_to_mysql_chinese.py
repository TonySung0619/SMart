import pymysql, time
import pandas as pd
from read_db_credentials import *
import concurrent.futures

def to_mysql(df_send):
    # 建立資料庫連線
    # 內部網路
    db = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        passwd=mysql_pswd,
        db='PData',
        port=mysql_port,
        charset='big5')

    db.ping(True) # 使用MySQL ping檢查連接，超時自動重新連接
    cursor = db.cursor() # 建立游標

    # ==================== 欄位名稱 ====================
    headers = list(df_send.columns)
    header_string = ''
    for each_header in headers:
        header_string += each_header + ', '

    header_string = header_string.rstrip(', ')

    # ==================== 輸入資料 ====================
    for rows in range(df_send.shape[0]):
        insert_list = []
        for columns in range(df_send.shape[1]):
            insert_list.append(df_send.iat[rows, columns])  # 產生一個insert_list

        # 處理資料格式
        insert_string = ''
        for each_element in insert_list:
            if each_element == 'NULL': # 進來的資料是NULL
                insert_string += each_element + ', '
            elif type(each_element) == str: # 進來的資料是XXXXXX(字串) -> 輸出形式變成'XXXXXX'
                insert_string = insert_string + "\'" + each_element + "\'" + ', '
            else: # 不是NULL
                each_element = str(each_element)
                insert_string = insert_string + each_element + ', '

        insert_string = insert_string.rstrip(', ') # 去掉最右邊的,
        #print(insert_string)


        # 寫入Mysql, 注意匯入Table是否正確
        try:
            sql_insert = 'INSERT INTO Dateconvert ({}) VALUES ({});'.format(header_string, insert_string)  # INSERT語法
            #print(sql_insert)
            cursor.execute(sql_insert)

            #print('成功寫入:{}'.format(sql_insert))

        except Exception as e:
            print('-' * 50)
            print(e)
            print('----------------error----------------{}'.format(sql_insert))
            print('-' * 50)

    db.commit()
    db.close()


if __name__ == '__main__':

    start = time.time()

    # # 讀取英文csv
    # df_send = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\0607\product_sub_translate_top300.csv')
    # 讀取中文csv
    df_send = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\0607\Dateconvert.csv', encoding='big5hkscs')


    # # 多工
    # # 針對多工的切片
    # row_count = df_send.shape[0]  # 資料筆數
    # div_num = 100000  # 切片單位大小
    # i = row_count // div_num  # 分i組, 針對餘數還要再分1組, 共i+1組
    #
    # df_send_cut_list = []
    # for i in range(i + 1):
    #     df_send_cut_list.append(df_send[i * div_num: (i + 1) * div_num])
    #
    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     results = executor.map(to_mysql, df_send_cut_list)


    # 不多工
    to_mysql(df_send)


    end = time.time()
    print(f'\nAll tasks completed in {end-start:.2f} seconds.')
