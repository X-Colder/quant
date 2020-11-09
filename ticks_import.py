import os
import pandas as pd
import pymongo

client = pymongo.MongoClient(host='localhost', port=27017)
db = client["ticks"]
col = db["ticks"]

BASEDIR = '/data/oracle/ticks'


def getDates():
    dates = os.listdir(BASEDIR)
    return dates


def getStocks(date):
    stocks = []
    files = os.listdir(BASEDIR + "/" + date)
    for file in files:
        stock = file.split(".")[0]
        stocks.append(stock)
    return stocks


def getTicks(date, stock):
    df = pd.read_csv(BASEDIR + "/" + date + "/" + stock + ".csv")
    df["Date"] = date
    df["StockID"] = stock
    data = df.to_dict(orient='records')
    try:
        col.insert_many(data)
        print("正在导入股票ID：%s, 日期：%s" % (stock, date))
    except Exception as e:
        print(e)


def main():
    dates = getDates()
    for date in dates:
        stocks = getStocks(date)
        for stock in stocks:
            getTicks(date, stock)


if __name__ == '__main__':
    main()
