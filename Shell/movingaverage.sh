#THe question I'm trying to answer is does a stock go up in price after a 2-3 week period after trading near its 50 day MA, 100 day MA, and/or 200 day MA?
#bash code to mine the Alpha Vantage API for long term daily price and moving average stock market data. We will need to gather two different datasets and combine the data into one file.

source ./av_keys.sh # get Alpha Vantage keys from key file or set environmental variables AV_KEY and AV_STUDENT KEY
mkdir avoverview

# get list of NYSE symbols and metadata
wget https://datahub.io/core/nyse-other-listings/r/nyse-listed.csv #may need updating this list is ~2 years old
#wget 'http://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download' #investigate


# collect symbol codes from nyse-listed.csv and wget the API url with an active API key:
cat nyse-listed.csv | sed "1d" | awk -F ',' '{print $1}'  | 

# wget the daily price data
xargs -P 4 -n 1 -I {} wget --output-document "avdata/"{}".csv" "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="{}&outputsize=full&apikey=$AV_STUDENT_KEY&datatype=csv

# wget the moving average data 50MA, 100MA, and 200MA
xargs -P 4 -n 1 -I {} wget --output-document "avdata/50MA/"{}".csv" "https://www.alphavantage.co/query?&function=SMA&interval=daily&time_period=50&series_type=open&apikey=$AV_STUDENT_KEY&datatype=csv&symbol="{}
xargs -P 4 -n 1 -I {} wget --output-document "avdata/100MA/"{}".csv" "https://www.alphavantage.co/query?&function=SMA&interval=daily&time_period=100&series_type=open&apikey=$AV_STUDENT_KEY&datatype=csv&symbol="{}
xargs -P 4 -n 1 -I {} wget --output-document "avdata/200MA/"{}".csv" "https://www.alphavantage.co/query?&function=SMA&interval=daily&time_period=200&series_type=open&apikey=$AV_STUDENT_KEY&datatype=csv&symbol="{}

ls avdata

# join tables together (will be using pseudocode since I don't know how to do this part yet)
tickersymbolfile.join(movingaveragefile, tickersymbol.timestamp == moveingaveragefile.time, "inner") 

#put avdata in the hadoop file system 
#assumes hadoop configured and accessible via hdfs 
hdfs dfs -put avdata

# Once all the data is gathered we can then move on the analysis part of the question. 
# More to come!
