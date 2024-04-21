import redis
import time
import json
import pytz
from datetime import datetime
import pandas as pd

F1M_CHANNEL = 'HNXDS:VN30F2405'
TIMEZONE = pytz.timezone('Asia/Ho_Chi_Minh')

class LiveTrading:
    def __init__(self,
                 func,
                 ):
        self.init_redis()
        self.func = func
        self.redis_data = {
            'Close': [],
            'Date': []
        }
        self.save_file = './trading_history.csv'
        self.data_file = './a.csv'
        self.resample_time = '30min'
        self.header = None
        self.previous_clue = 50
        self.current_index = None
        self.time = 30000

    def get_redis_processor(self, **kwargs):
        def run(redis_message):
            trading_df = self.data_processing(redis_message)

            if trading_df is not None:
                output = self.func(trading_df, **kwargs).iloc[-1:]
                print(output)
                self.save_trading_history(output)

        return run
    
    def save_trading_history(self, history):
        data = history.to_csv().split('\n')

        if self.header == None:
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
            return_df.set_index('Date', inplace=True)

            return_df = return_df.resample(self.resample_time, closed='right', label='right').agg({'Close': 'last'})
            return_df.dropna(inplace=True)
            return_df = return_df[-self.previous_clue-1:-1]

            if pd.isna(return_df['Close'].iloc[-1]):
                return None

            try:
                if str(return_df.index[-1]) != self.current_index:
                    self.current_index = str(return_df.index[-1])
                    return return_df
            except Exception as e:
                print(return_df)
                print('error', '*'*20)
                print(e)

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

    def __call__(self, trading_df, **kwargs):
        if trading_df is not None:
            self.init_data(trading_df)

        pub_sub = self.redis_client.pubsub()
        print('start subscribing')
        pub_sub.psubscribe(**{F1M_CHANNEL: self.get_redis_processor(**kwargs)})
        pubsub_thread = pub_sub.run_in_thread(sleep_time=1)
        time.sleep(self.time)
        pubsub_thread.stop()