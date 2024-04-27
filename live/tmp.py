import redis
import time
import json
import pandas as pd
import pytz
from datetime import datetime
import os
from ..data.vn30f1m_algotrade import get_dataframe, fix_dataframe

F1M_CHANNEL = 'VN30F1M05'#'HNXDS:VN30F2405'
TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')

class LiveAlgoTrading:
    def __init__(self,
                history_save_file = './trading_history.csv',
                past_trading_data_file = './a.csv',
                dataframe_resample_time = '30min',
                number_of_used_trading_data = 40,
                trading_waiting_time = 30000,
                 ):
        self.init_redis()
        self.func = None
        self.redis_data = {
            'Close': [],
            'Date': []
        }
        self.save_file = history_save_file
        self.data_file = past_trading_data_file
        self.resample_time = dataframe_resample_time
        self.previous_clue = number_of_used_trading_data
        self.current_index = None
        self.time = trading_waiting_time

        if os.path.exists(history_save_file):
            self.header = True
        else:
            self.header = False

    def get_redis_processor(self, **kwargs):
        def run(redis_message):
            trading_df = self.data_processing(redis_message)

            if trading_df is not None:
                output = self.func(trading_df, **kwargs).iloc[-1:]
                print(output)
                print('-'*20)
                self.save_trading_history(output)

        return run
    
    def save_trading_history(self, history):
        data = history.to_csv().split('\n')

        if not self.header:
            self.header = data[0]
            data = data[1] + '\n'
            with open(self.save_file, 'w') as f:
                f.write(self.header + '\n' + data)
        else:
            data = data[1] + '\n'
            with open(self.save_file, 'a') as f:
                f.write(data)

    def save_data_from_redis_message(self, redis_message):
        data = json.loads(redis_message['data'])
        Close = data['latest_matched_price']
        dtime = str(datetime.fromtimestamp(data['timestamp']).astimezone(TIMEZONE).replace(tzinfo=None)) + '.000001'
        tickersymbol = data['instrument'].split(':')[-1]
        quantity = data['bid_quantity_1']
        
        with open(self.data_file, 'a') as af:
            af.write(f'{dtime},{tickersymbol},{Close},{quantity}\n')

    def data_processing(self, redis_message):
        trading_data_full = json.loads(redis_message['data'])

        Close = trading_data_full['latest_matched_price']
        dtime = datetime.fromtimestamp(trading_data_full['timestamp']).astimezone(TIMEZONE).replace(tzinfo=None)

        if Close != None:
            self.save_data_from_redis_message(redis_message)
            self.redis_data['Close'].append(Close)
            self.redis_data['Date'].append(dtime)

            return_df = pd.DataFrame.from_dict(self.redis_data)
            return_df = fix_dataframe(return_df, index_name='Date', Trading='Close', resample_time=self.resample_time)
            
            checking_df = return_df.copy(deep=True)
            return_df = return_df[-self.previous_clue-1:-1]

            if str(return_df.index[-1]) != self.current_index:
                self.current_index = str(return_df.index[-1])
                print('time taking', self.current_index)
                print('time last', checking_df.index[-1])
                print('real time', dtime)
                return return_df.copy(deep=True)
        
        return None

    def init_data(self, trading_df):
        self.redis_data['Close'] = trading_df['Close'].to_list()
        self.redis_data['Date'] = trading_df.index.to_list()

    def init_redis(self):
        redis_host = '52.76.242.46'
        redis_port = 6380
        redis_password = 'Vn9ZMBF5SLafGkqEWc4h3b'

        # connect to redis server
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password
        )

        # check connection to redis OK
        print(self.redis_client.ping())

    def __call__(self, func):
        self.func = func

        def wrapper(**kwargs):
            df = get_dataframe(self.data_file, resample_time=self.resample_time)
            self.init_data(df)

            pub_sub = self.redis_client.pubsub()
            print('start subscribing')
            pub_sub.psubscribe(**{F1M_CHANNEL: self.get_redis_processor(**kwargs)})
            pubsub_thread = pub_sub.run_in_thread(sleep_time=1)
            time.sleep(self.time)
            pubsub_thread.stop()

        return wrapper