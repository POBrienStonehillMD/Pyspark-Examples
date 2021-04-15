  
#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

spark = SparkSession.builder \
    .master("local[8]") \
    .appName("avdata") \
    .getOrCreate()
    
# warning: It is a little slower to read the data with inferSchema='True'. 
#Consider defining schema.

df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.printSchema()

#get a count of rows
df.count()
#counts the rows from all the avdata data files.

# get sourcefile name from input_file_name()
df = df.withColumn("path", fun.input_file_name())
regex_str = "[\/]([^\/]+[^\/]+)$" #regex to extract text after the last / or \
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()
# this code returns whats in the source file. I.E. timestamp, open, high, low, 
# close, adjusted_close, volume, dividend_amount, split_coefficient, path, sourcefile

#######################################################################
# handle dates and times
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df.show(2)
# code formats 'timestamp' to a date value and returns the first 2 rows in the stockticker IP file

# now we should be able to convert or extract date features from timestamp
df.withColumn('dayofmonth', fun.dayofmonth("timestamp")).show(2)
df.withColumn('month', fun.month("timestamp")).show(2)
df.withColumn('year', fun.year("timestamp")).show(2)
df.withColumn('dayofyear', fun.dayofyear("timestamp")).show(2) 
# each line creates a new column, the first line creates a new column with the day of the month,
# the second line creates a new column with the month value, the third line creates a new column
# with the year value, and the last line creates a new column with the day of the year value

# calculate the difference from the current date ('days_ago')
df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show()
# creates a new column that takes the timestamp date and tells you how manys day ago
# that was compared to the current date. 


########################################################################
#group_by
# summarize within group data
df.groupBy("sourcefile").count().show(99)
# counts how many rows are in the sourcefile per stock tickers. displays the first 99 stocks.
df.groupBy("sourcefile").min('open').show(99)
# finds the minimum opening value for each stock tickers. displays the first 99 stocks.
df.groupBy("sourcefile").mean('open').show(99)
# finds the mean open value for each stock tickers. displays the first 99 stocks.
df.groupBy("sourcefile").max('open','close').show(99)
# finds the maximum open value for each of the stock tickers. displays the first 99 stocks.



########################################################################
#window functions
from pyspark.sql.window import Window
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

windowSpec  = Window.partitionBy("sourcefile").orderBy("days_ago")

#see also lead
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
dflag.select('sourcefile', 'lag', 'open').show(99)
# creates a table with columns sourcefile, lag, and open values. The sourcefile will be our
# stock tickers. The lag takes the price of the previous open that was 14 days ago. The open
# value is the opening stock price that day. 

dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open')).show() 
# output all columns as well as adding a new column which is the difference between the lag value
# and the opening price value. 
