import pandas as pd
import time

# 此檔案用來清理product檔中DEPARTMENT為空白的資料
# 此種資料:DEPARTMENT/COMMODITY_DESC/SUB_COMMODITY_DESC/CURR_SIZE_OF_PRODUCT 4欄位會同時為空白

t1 = time.time()

# 讀取csv
df_clean_null = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\product.csv')

print('清理前資料筆數:{}'.format(df_clean_null.shape[0]))

def clean_null(x):
    if x == ' ': # 事實上資料為'一個space'
        print('Oops')
        return None
    else :
        return x
df_clean_null['DEPARTMENT'] = df_clean_null['DEPARTMENT'].apply(clean_null)

df_clean_null.dropna(inplace=True)

print('清理後資料筆數:{}'.format(df_clean_null.shape[0]))

df_clean_null.to_csv('product_v1.csv',encoding='utf-8-sig', index=False)

t2 = time.time()

print('take {} seconds'.format(t2-t1))

