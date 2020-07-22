import pandas as pd
from datetime import date
from datetime import timedelta

# 此檔案用來處理交易檔中奇怪的資料，功能分成很多部分

# v2 貨品單價 = ((SALES_VALUE) - (RETAIL_DISCOUNT) - (COUPON_MATCH_DISCOUNT)) / QUANTITY

# 讀原始交易檔
df_trans = pd.read_csv('C:/Users/Big data/Desktop/project/database/transaction_data.csv')
print(df_trans.shape)

# 刪除retail_discount > 0的資料
# 將retail_discount > 0的資料篩選出來，放入df_trans_retail_false的dataframe
df_trans_retail_false = df_trans[df_trans['RETAIL_DISC']>0]
# 將df_trans_retail_false的index存成list型態
df_trans_retail_false_index_list = list(df_trans_retail_false.index)
# 刪除retail_discount > 0的資料
df_trans.drop(df_trans_retail_false_index_list, inplace=True)
# 統計個數
print('retail_false_count:{}'.format(len(df_trans_retail_false_index_list)))

# 刪除quantity = 0的資料
# 將quantity = 0的資料篩選出來，放入df_trans_quantity_false的dataframe
df_trans_quantity_false = df_trans[df_trans['QUANTITY']==0]
# 將df_trans_quantity_false的index存成list型態
df_trans_quantity_false_index_list = list(df_trans_quantity_false.index)
# 刪除quantity = 0的資料
df_trans.drop(df_trans_quantity_false_index_list, inplace=True)
# 統計個數
print('quantity_false_count:{}'.format(len(df_trans_quantity_false_index_list)))

# 生成Product_single_price欄位，再刪除異常值(單價<0 or 單價=0)
df_trans['Product_single_price'] = \
    (df_trans['SALES_VALUE'] - df_trans['RETAIL_DISC'] - df_trans['COUPON_MATCH_DISC']) / df_trans['QUANTITY']

# 將Product_single_price = 0的資料篩選出來，放入df_trans_psp_false_0的dataframe
df_trans_psp_false = df_trans[df_trans['Product_single_price']==0]
# 將df_trans_psp_false_0的index存成list型態
df_trans_psp_false_index_list = list(df_trans_psp_false.index)
# 刪除Product_single_price = 0的資料
df_trans.drop(df_trans_psp_false_index_list, inplace=True)
# 統計個數
print('psp_false_count:{}'.format(len(df_trans_psp_false_index_list)))

# 產生星期
def day_xform_weekday(DAY):
    if DAY % 7 == 6:
        return 'Sun'
    elif DAY % 7 == 0:
        return 'Mon'
    elif DAY % 7 == 1:
        return 'Tue'
    elif DAY % 7 == 2:
        return 'Wed'
    elif DAY % 7 == 3:
        return 'Thu'
    elif DAY % 7 == 4:
        return 'Fri'
    elif DAY % 7 == 5:
        return 'Sat'

# 產生一個Week_day欄位，並且依照day_xform_weekday(欄位DAY)的結果塞入值
df_trans['Week_day'] = df_trans['DAY'].apply(day_xform_weekday)

# 產生年/月/日
def day_plus(DAY):
    initial_date = date(2018, 12, 31)  # 設定起始日期2018/12/31
    delta = timedelta(days=1) * DAY # 設定時間間隔為1天，並且乘以欄位DAY的值
    cal_date = initial_date + delta
    return str(cal_date) # 設成str型態，讓之後能與時間合併

# 產生一個Date欄位，並且依照day_plus(欄位DAY)的結果塞入值
df_trans['Date'] = df_trans['DAY'].apply(day_plus)



# 將時間格式都轉成'xx:xx'
def clock_time(TRANS_TIME):
    TRANS_TIME = str(TRANS_TIME)
    if len(TRANS_TIME) == 4:
        return TRANS_TIME[:2] + ":" + TRANS_TIME[-2:]
    elif len(TRANS_TIME) == 3:
        return "0" + TRANS_TIME[:1] + ":" + TRANS_TIME[-2:]
    elif len(TRANS_TIME) == 2:
        return "00:" + TRANS_TIME
    else:
        return "00:0" + TRANS_TIME

# 將TRANS_TIME欄位的值都代換
df_trans['TRANS_TIME'] = df_trans['TRANS_TIME'].apply(clock_time)

# 日期/時間格式合併到Detail_time欄位
df_trans['Detail_time'] = df_trans['Date'] + ' ' + df_trans['TRANS_TIME']

# 存檔 & 輸出
print(df_trans.shape)
df_trans.to_csv('transaction_data_v1.csv',encoding='utf-8-sig', index=False)
