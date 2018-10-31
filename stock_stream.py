from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import requests
import api_credentials

conf = SparkConf().setAppName('stock_stream').setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)

'''Streaming Data'''
''' Supports 1min, 5min, 15min, 30min, 60min'''
apikey3 = api_credentials.stream_key
stock_symbol_3 = 'BABA'
minutes = '15'
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+stock_symbol_3+'&interval='+minutes+'min&outputsize=full&apikey='+apikey3
response = requests.get(url)
file = response.json()
Time_Stamp = file['Time Series ('+minutes+'min)']
data = []
for i in Time_Stamp:
    open = Time_Stamp[i]['1. open']
    high = Time_Stamp[i]['2. high']
    low = Time_Stamp[i]['3. low']
    close = Time_Stamp[i]['4. close']
    volume = Time_Stamp[i]['5. volume']

    data.append([i, open, high, low, close, volume])
schema = StructType([StructField("Timestamp", StringType(), True),
                     StructField("Open", StringType(), True),
                     StructField("High", StringType(), True),
                     StructField("Low", StringType(), True),
                     StructField("Close", StringType(), True),
                     StructField("Volume", StringType(), True)])
df = sqlc.createDataFrame(data, schema=schema)
df.show()

''' TODO::::Stream every minute '''
