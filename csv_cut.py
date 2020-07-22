import pandas as pd

# 此用來為交易檔案切片，測試程式

# 讀取csv
df_cut = pd.read_csv('C:/Users/Big data/Desktop/project/database/Arranged/cut_data/transaction_data_v2_1.csv')

# 針對多工的切片
row_count = df_cut.shape[0]  # 資料筆數
div_num = 1000  # 切片單位大小
i = row_count // div_num  # 分i組, 針對餘數還要再分1組, 共i+1組

df_cut_list = []
for i in range(i + 1):
    df_cut_list.append(df_cut[i * div_num: (i + 1) * div_num])
    break

print(df_cut_list)
j = 0
for each_cut in df_cut_list:
    j += 1
    each_cut.to_csv('transaction_data_v2_{}_{}.csv'.format(div_num,j), encoding='utf-8-sig', index=False)