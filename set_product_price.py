import pandas as pd
import concurrent.futures
import time

# 此檔案用來統一交易檔中，相同物品擁有不同價格的問題

def set_price(id):
    print(id)

    id_filter = (df['PRODUCT_ID'] == id)

    # id個數只有一個，不做事
    if len(df[id_filter]) <= 1:
        return
    else: # id個數不只一個，生成一個字典，內容為 {價格:出現次數}
        id_price_dict = {}
        for any_price in df[id_filter]['Product_single_price']:
            if any_price not in id_price_dict:
                id_price_dict[any_price] = 1
            else :
                id_price_dict[any_price] += 1

    # 價格字典排序 & 取值
    id_price_dict_list_sorted = sorted(id_price_dict.items(), key=lambda x: x[1], reverse=True)
    max_count_price = id_price_dict_list_sorted[0][0]
    # 將其他價格代換為眾數價格
    df.loc[id_filter, 'Product_single_price'] = max_count_price

    return

if __name__ == "__main__":
    t1 = time.time()

    # 讀取交易檔(要改價格的檔案)，並且取出PRODUCT_ID
    global df
    df = pd.read_csv(r'C:\Users\Big data\Desktop\project\database\Arranged\0607\transaction_data_v2.csv')
    pid_list = list(set((df['PRODUCT_ID'])))
    pid_list.sort()

    # 多工會出錯(同時編輯相同檔案，有些資料會沒改到)
    # with concurrent.futures.ProcessPoolExecutor() as ex :
    #     ex.map(set_price,pid_list)

    # 將各ID送入set_price()
    for each_id in pid_list:
        set_price(each_id)

    df.to_csv('transaction_data_v3', encoding='utf-8-sig',index=False)
    t2 = time.time()
    print(f'\ntake {t2-t1:.4f} seconds')



