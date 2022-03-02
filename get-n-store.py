import yfinance as yf
import mysql.connector
from decouple import config


def get_stock_data(ticker):
    stock_data = yf.download(tickers=ticker, period="1d", interval="1m")
    stock_data.reset_index(inplace=True)
    latest_price = stock_data.iloc[-1:]
    record = latest_price.to_records(index=False)
    result = list(record)

    values = []
    for value in result[0]:
        values.append(value)

    values = values[1:6]
    values.insert(0, ticker)
    inserted_val = tuple(values)

    return inserted_val

def insert_to_db(ticker, data, db_conn, db_cursor):
    query = 'INSERT INTO '+config('database')+'.stock_history (ticker, open, high, low, close, adj_close) VALUES (%s, %s, %s, %s, %s, %s)'

    db_cursor.execute(query, data)

    db_conn.commit()

    return ticker+' data inserted'

    

if __name__ == "__main__":
    stocks_symbol = ['INDF.JK', 'BMRI.JK', 'UNVR.JK', 'BBCA.JK']

    mydb = mysql.connector.connect(
        host=config('host'),
        user=config('user'),
        password=config('password'),
        database=config('database')
    )

    mycursor = mydb.cursor()

    for stock in stocks_symbol:
        raw_data = get_stock_data(stock)
        print(insert_to_db(stock, raw_data, mydb, mycursor))
        