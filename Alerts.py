from datetime import datetime, timedelta
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230220',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

my_token = '6116887711:AAHlmLXMeSAB6040WfJ5QiziVZGH60CG6fM' 
bot = telegram.Bot(token = my_token) 

chat_id =  393828586

def check_anomaly(df, metric, a=4, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods = 1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods = 1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

def run_alerts(data, metrics_list):

    for metric in metrics_list:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)

        if is_alert == 1:
            msg = '''Метрика {metric}:\nтекущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}'''.format(metric=metric, current_val=df[metric].iloc[-1], last_val_diff=1 - (df[metric].iloc[-1] / df[metric].iloc[-2]))
            
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time')
            ax.set(ylabel='')
             
            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            bot.sendMessage(chat_id=chat_id, text=msg)

default_args = {
    'owner': 'd-jarusov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 27),
}

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_djarusov():
    
    @task
    def feed_alerts():
        query1 = """SELECT 
                        toStartOfFifteenMinutes(time) as ts,
                        toDate(time) as date,
                        formatDateTime(ts, '%R') as hm,
                        uniqExact(user_id) as users_feed, 
                        countIf(user_id, action = 'like') likes,
                        countIf(user_id, action = 'view') views,
                        round(likes/views, 3) ctr
                    FROM simulator_20230220.feed_actions
                    WHERE time >= yesterday() and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm"""
        
        data =  ph.read_clickhouse(query=query1, connection=connection)
        metrics_list = ['users_feed', 'views', 'likes', 'ctr']       
        run_alerts(data, metrics_list)
        
    @task
    def message_alerts():
        query2 = """SELECT 
                        toStartOfFifteenMinutes(time) as ts,
                        toDate(time) as date,
                        formatDateTime(ts,'%R') as hm,
                        uniqExact(user_id) as users_message, 
                        count(user_id) messages
                    FROM simulator_20230220.message_actions
                    WHERE time >= yesterday()-1 and time < toStartOfFifteenMinutes(now())
                    GROUP BY ts, date, hm"""

        df =  ph.read_clickhouse(query=query2, connection=connection)
        
        metrics_list = ['users_message', 'messages']
        run_alerts(df, metrics_list)

    feed_alerts()
    message_alerts()    
    
alerts_djarusov=alerts_djarusov()



