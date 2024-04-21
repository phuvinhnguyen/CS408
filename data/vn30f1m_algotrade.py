import psycopg2
from dateutil.relativedelta import relativedelta, TH
from datetime import datetime, timedelta
import csv

def get_data(datafile):
    # Database connection parameters
    db_params = {
        'host': 'api.algotrade.vn',
        'port': 5432,
        'database': 'algotradeDB',
        'user': 'intern_read_only',
        'password': 'Bingo@0711'
    }

    def get_third_thursday(year, month):
        # Calculate the third Thursday of the month
        first_day = datetime(year, month, 1)
        third_thursday = first_day + relativedelta(day=1, weekday=TH(3))
        return third_thursday

    def get_data_for_month(year, month):
        # Database connection
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Calculate start and end dates based on the third Thursday of the month
        start_date = get_third_thursday(year if month > 1 else year - 1, month - 1 if month > 1 else 12)
        end_date = get_third_thursday(year, month)

        ticker_symbol = 'VN30F' + str(year)[-2:] + str(month).zfill(2)

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

        print(start_date, end_date)

        # Execute the query
        cursor.execute(query)

        # Fetch all rows
        rows = cursor.fetchall()

        # Close the connection
        cursor.close()
        connection.close()

        return rows

    # Example: Get data for each month in the past for ticker symbol 'VN30F2310'
    def generate_month_list(start_year, start_month):
        current_date = datetime.now()
        start_date = datetime(start_year, start_month, 1)

        year_month_list = []

        while start_date <= current_date:
            year_month_list.append((start_date.year, start_date.month))
            start_date += timedelta(days=31)  # Add a month, assuming a month has at most 31 days

        return year_month_list

    with open(datafile, 'w') as rf:
        csv_writer = csv.writer(rf)
        csv_writer.writerow(['datetime', 'tickersymbol', 'price', 'quantity'])

    for year, month in generate_month_list(2018, 1):  # Get data for the last 6 months
        datas = get_data_for_month(year, month)

        with open(datafile, 'a') as rf:
            csv_writer = csv.writer(rf)
            csv_writer.writerows(datas)