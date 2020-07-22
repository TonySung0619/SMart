from flask import Flask, render_template, request, redirect, url_for
import select_trans_multi_thread_v3 as stmt
import pandas as pd
import datetime

from time import time

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def homepage():
    return render_template('index6.html')

@app.route('/history_select', methods=['GET', 'POST'])
def history_query():
    if request.method == 'POST':
        datestart = request.form.get('datestart')
        dateend = request.form.get('dateend')
        dateend_c = datetime.datetime.strptime(datestart, "%Y-%m-%dT%H:%M")

        # 不是寫23:59是因為，kibana會自動把送進去的時間+8小時
        if dateend_c > datetime.datetime.today():
            dateend = str(datetime.datetime.today())[0:10] + 'T15:59'

        datestart_k = str(datetime.datetime.strptime(datestart, "%Y-%m-%dT%H:%M") - datetime.timedelta(hours = 8))
        dateend_k = str(datetime.datetime.strptime(dateend, "%Y-%m-%dT%H:%M") - datetime.timedelta(hours = 8))
        datestart_k = datestart_k[0:10] + 'T' + datestart_k[11:19]
        dateend_k = dateend_k[0:10] + 'T' + dateend_k[11:19]

        # 送入stmt函式，去Mysql抓資料
        datestart_sql = datestart.replace('T',' ')
        dateend_sql = dateend.replace('T',' ')
        result_list = stmt.trans_by_day(datestart_sql, dateend_sql)
        data_org = result_list[0]
        data_count = result_list[1]
        data_on_web = result_list[2]
        csv_name = result_list[3]

        return render_template('transaction4.html', tables=[data_on_web.to_html(classes='data', col_space=80, justify='center')], data_count=data_count,
                               datestart_sql=datestart_sql, dateend_sql=dateend_sql, datestart_k = datestart_k, dateend_k = dateend_k, csv_name = csv_name)

    else:
        return render_template('history_select.html')


@app.route('/realtime', methods=['GET', 'POST'])
def realtime():
    return render_template('realtime.html')


@app.route('/rfm', methods=['GET', 'POST'])
def rfm():
    return render_template('rfm.html')

@app.route('/realtime_demo', methods=['GET', 'POST'])
def realttime_demo():
    return render_template('realtime_demo.html')

@app.route('/text_mining', methods=['GET', 'POST'])
def text_mining():
    return render_template('text_mining.html')

@app.route('/customer', methods=['GET', 'POST'])
def customer():
    return render_template('customer.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
