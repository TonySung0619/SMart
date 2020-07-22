import pandas as pd
import numpy as np
import time

# 此py檔為清除主類別:"COUPON/MISC ITEMS" 子類別是:"GASOLINE-REG UNLEADED"的奇怪資料 & ID = 5716076 / 731106 (數量異常)

t1 = time.time()

# 讀取商品檔csv
df_clean_sq = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\0607\product_v1.csv')

print('清除前資料筆數為:{}'.format(df_clean_sq.shape[0]))


# 取得主類別:'COUPON/MISC ITEMS'，子類別:'GASOLINE-REG UNLEADED' 的PRODUCT_ID
filter1 = (df_clean_sq['COMMODITY_DESC'] == 'COUPON/MISC ITEMS')
filter2 = (df_clean_sq['SUB_COMMODITY_DESC'] == 'GASOLINE-REG UNLEADED')
df_clean_sq_bad_list = list(df_clean_sq[filter1][filter2]['PRODUCT_ID'])

# 5716076.731106 商品數量也很怪，將它加進去bad_list
df_clean_sq_bad_list.append(5716076)
df_clean_sq_bad_list.append(731106)
print(df_clean_sq_bad_list)
print(len(df_clean_sq_bad_list))


# 刪除資料
total_count = 0
for each_id in df_clean_sq_bad_list:
    df_clean_sq_bad2 = df_clean_sq[df_clean_sq['PRODUCT_ID'] == (each_id)]
    df_clean_sq_bad2_index_list = list(df_clean_sq_bad2.index)
    df_clean_sq.drop(df_clean_sq_bad2_index_list, inplace=True)
    total_count += len(df_clean_sq_bad2_index_list)

print('清除前資料筆數為:{}'.format(df_clean_sq.shape[0]))

print('總共刪除了{}筆資料'.format(total_count))

df_clean_sq.to_csv('product_v2.csv',encoding='utf-8-sig', index=False)

t2 = time.time()

print('take {} seconds'.format(t2-t1))