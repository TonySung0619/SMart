import pandas as pd
import time

# 此py檔為清除主類別:"COUPON/MISC ITEMS" 子類別是:"GASOLINE-REG UNLEADED"的奇怪資料 & ID = 5716076 / 731106 (數量異常)

t1 = time.time()

# 取出主類別為'COUPON/MISC ITEMS'，子類別為'GASOLINE-REG UNLEADED'的PRODUCT_ID
df_NGID = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\product_no_unit_clean_null_clean_quantity.csv')
# 第一個篩選條件 : 主類別:'COUPON/MISC ITEMS'
NGID_filter1 = (df_NGID['COMMODITY_DESC'] == 'COUPON/MISC ITEMS')
# 第二個篩選條件 : 子類別: 'GASOLINE-REG UNLEADED'
NGID_filter2 = (df_NGID['SUB_COMMODITY_DESC'] == 'GASOLINE-REG UNLEADED')
df_NGID = df_NGID[NGID_filter1][NGID_filter2]
NGID_list = list(df_NGID['PRODUCT_ID'])


# 讀取交易檔
df_clean_gas = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\0607\transaction_data_v1.csv')
print('清除前資料筆數為:{}'.format(df_clean_gas.shape[0]))


total_count = 0
# 刪除主類別:'COUPON/MISC ITEMS'，子類別:'GASOLINE-REG UNLEADED'的資料
for each_id in NGID_list:
    df_clean_gas1 = df_clean_gas[df_clean_gas['PRODUCT_ID']==int(each_id)]
    df_clean_gas1_index_list = list(df_clean_gas1.index)
    df_clean_gas.drop(df_clean_gas1_index_list, inplace=True)
    print(each_id, 'count:{}'.format(len(df_clean_gas1_index_list)))
    total_count += len(df_clean_gas1_index_list)


# 5716076 雖子類別為FUEL，但數量也很奇怪 -> 刪掉
df_clean_gas2 = df_clean_gas[df_clean_gas['PRODUCT_ID']==5716076]
df_clean_gas2_index_list = list(df_clean_gas2.index)
df_clean_gas.drop(df_clean_gas2_index_list,inplace=True)
print('5716076 count:{}'.format(len(df_clean_gas2_index_list)))

# 731106 雖子類別為FUEL，但數量也很奇怪 -> 刪掉
df_clean_gas3 = df_clean_gas[df_clean_gas['PRODUCT_ID']==731106]
df_clean_gas3_index_list = list(df_clean_gas3.index)
df_clean_gas.drop(df_clean_gas3_index_list,inplace=True)
print('731106 count:{}'.format(len(df_clean_gas3_index_list)))


print('NG_total_count:{}'.format(total_count+len(df_clean_gas2_index_list)+len(df_clean_gas3_index_list)))

print('清除後資料筆數為:{}'.format(df_clean_gas.shape[0]))


df_clean_gas.to_csv('transaction_data_v2.csv',encoding='utf-8-sig', index=False)

t2 = time.time()

print(f'\ntake {t2-t1:.4f} seconds')