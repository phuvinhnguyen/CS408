import psycopg2
from dateutil.relativedelta import relativedelta, TH
from datetime import datetime, timedelta
import csv
import pandas as pd

def get_dataframe(file_name, index_name = 'datetime', Trading = 'price', resample_time='30min'):
    morning_start = pd.Timestamp('8:45').time()
    morning_end = pd.Timestamp('11:00').time()
    afternoon_start = pd.Timestamp('13:00').time()
    afternoon_end = pd.Timestamp('14:45').time()

    df = pd.read_csv(file_name)
    df[index_name] = pd.to_datetime(df[index_name], format='mixed')
    df.set_index(index_name, inplace=True)
    df = df.resample(resample_time, closed='right', label='right', offset=pd.Timedelta(minutes=-1)).agg({Trading: 'last'})
    df.rename(columns={Trading: 'Close'}, inplace=True)
    df.dropna(inplace=True)

    df = df.loc[
        ((df.index.time >= morning_start) & (df.index.time <= morning_end)) |
        ((df.index.time >= afternoon_start) & (df.index.time <= afternoon_end))
    ]

    return df

def get_third_thursday(year, month):
    # Calculate the third Thursday of the month
    first_day = datetime(year, month, 1)
    third_thursday = first_day + relativedelta(day=1, weekday=TH(3))
    return third_thursday

# Example: Get data for each month in the past for ticker symbol 'VN30F2310'
def generate_month_list(start_year, start_month):
    current_date = datetime.now()
    start_date = datetime(start_year, start_month, 1)

    year_month_list = []

    while start_date <= current_date:
        year_month_list.append((start_date.year, start_date.month))
        start_date += timedelta(days=31)  # Add a month, assuming a month has at most 31 days

    return year_month_list

def get_data(datafile, start_year=2018, start_month=1):
    # Database connection parameters
    db_params = {
        'host': 'api.algotrade.vn',
        'port': 5432,
        'database': 'algotradeDB',
        'user': 'intern_read_only',
        'password': 'Bingo@0711'
    }

    def get_data_for_month(year, month):
        # Database connection
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Calculate start and end dates based on the third Thursday of the month
        start_date = get_third_thursday(year - 1 if month == 1 else year, 12 if month == 1 else month - 1)
        end_date = get_third_thursday(year, month)

        ticker_symbol = 'VN30F' + str(year)[-2:] + str(month).zfill(2)

        print(ticker_symbol, start_date, end_date)

        # SQL query
        query = f"""
            SELECT m.datetime, m.tickersymbol, m.price, v.quantity
            FROM "quote"."matched" m
            LEFT JOIN "quote".total v
            ON m.tickersymbol = v.tickersymbol
            AND m.datetime = v.datetime
            WHERE m.datetime BETWEEN '{start_date}' AND '{end_date}'
            AND m.tickersymbol = '{ticker_symbol}';
        """

        # Execute the query
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Close the connection
        cursor.close()
        connection.close()

        return rows

    with open(datafile, 'w') as rf:
        csv_writer = csv.writer(rf)
        csv_writer.writerow(['datetime', 'tickersymbol', 'price', 'quantity'])

    for year, month in generate_month_list(start_year, start_month):  # Get data for the last 6 months
        datas = get_data_for_month(year, month)

        with open(datafile, 'a') as rf:
            csv_writer = csv.writer(rf)
            csv_writer.writerows(datas)