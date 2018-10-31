from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import col
import requests
import pandas as pd
import datetime
import api_credentials

conf = SparkConf().setAppName('stock_analysis').setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)
# spark = SparkSession.builder.master('master').appName('stock_analysis').getOrCreate()

def percentage_change(x,y):

    if x > y:
        c = ((x-y)/y)*100
        return c
    elif y > x:
        c = ((y-x)/x)*100
        return c
    else:
        c = 0
        return c

def square(x):
    return x**2


'''  Security 1  '''
stock_symbol_1 = 'FB'
apikey = api_credentials.google_api
url = ('https://marketdata.websol.barchart.com/getHistory.json?apikey='+apikey+'&symbol='+stock_symbol_1+'&type=daily&startDate=20160608000000')
response = requests.get(url)
file = response.json()
# print(file)
results = file['results']
L = []
data = []
list(map(lambda x: data.append(list(x.values())), results))
list(map(lambda x: print(x), data))

''' Security 2 '''
stock_symbol_2 = 'TSLA'
url2 = ('https://marketdata.websol.barchart.com/getHistory.json?apikey='+apikey+'&symbol='+stock_symbol_2+'&type=daily&startDate=20160608000000')
response2 = requests.get(url2)
file2 = response2.json()
results2 = file2['results']
data2 = []
list(map(lambda x: data2.append(list(x.values())), results2))
list(map(lambda x: print(x), data2))

df = pd.DataFrame(data, columns=["Symbol", "Timestamp", "TradingDay", "Open", "High", "Low",  "Close", "Volume", "OpenInterest"])
# df.to_csv(stock_symbol+str(datetime.datetime.now())+'.csv', sep=',')
schema = StructType([StructField("Symbol", StringType(), True),
                    StructField("Timestamp", StringType(), True),
                    StructField("TradingDay", StringType(), True),
                     StructField("Open", StringType(), True),
                     StructField("High", StringType(), True),
                     StructField("Low", StringType(), True),
                     StructField("Close", StringType(), True),
                     StructField("Volume", StringType(), True),
                     StructField("OpenInterest", StringType(), True)])
df = sqlc.createDataFrame(data, schema=schema)
df = df.withColumn("Date", f.regexp_replace("TradingDay", '-', '/'))

pc = df.withColumn("%_Change", f.round(((df['Close']-df['Open'])/df['Open'])*100, 2))
pc = pc.sort(f.desc('Date'))
Percent_change_1 = pc
variance_1 = pc.select(square(f.stddev(f.col('%_Change'))).alias('Variance')).show()
# pc.selectExpr('Variance', square(col('Standard_Deviation'))).show()




df2 = pd.DataFrame(data2, columns=["Symbol", "Timestamp", "TradingDay", "Open", "High", "Low",  "Close", "Volume", "OpenInterest"])
# df.to_csv(stock_symbol+str(datetime.datetime.now())+'.csv', sep=',')
df2 = sqlc.createDataFrame(data2, schema=schema)
df2 = df2.withColumn("Date", f.regexp_replace("TradingDay", '-', '/'))
pc2 = df2.withColumn("%_Change", f.round(((df2['Close']-df2['Open'])/df2['Open'])*100, 2))
pc2 = pc2.sort(f.desc('Date'))
percent_change_2 = pc2
variance_2 = pc2.select(square(f.stddev(f.col('%_Change'))).alias('Variance')).show()

# pc.selectExpr('Variance', square(col('Standard_Deviation'))).show()
