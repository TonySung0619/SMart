import pickle

# 第一次跑elk_v2程式，會需要先生成pickle_time.txt & pickle_id.txt

# 讀上一次查詢，最後一筆資料的時間
enddate = '2020-07-17 16:56' # 第一次使用，自行設定

with open('pickle_time.txt', 'wb') as ft:
    pickle.dump(enddate, ft)


# 讀上一次查詢，最後一筆的id (for elastic use)
id = '2319334' # 第一次使用，自行設定

with open('pickle_id.txt', 'wb') as f:
    pickle.dump(id, f)