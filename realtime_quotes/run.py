from pymongo import UpdateOne
from database import DB_CONN
import tushare as ts
from datetime import datetime



df = ts.get_realtime_quotes('000581')
print(df.index)

