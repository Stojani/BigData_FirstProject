set hive.auto.convert.join = false;

CREATE TABLE IF NOT EXISTS prices (ticker STRING,
                                    open DOUBLE,
                                    close DOUBLE,
                                    adj_close DOUBLE,
                                    low DOUBLE,
                                    high DOUBLE,
                                    volume INT,
                                    ticker_date DATE)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' 
OVERWRITE INTO TABLE prices;

CREATE TABLE IF NOT EXISTS stocks (ticker STRING,
                                    stock_exchange STRING,
                                    name STRING,
                                    sector STRING,
                                    industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"")
STORED AS TEXTFILE
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hadoop/historical_stocks.csv' 
OVERWRITE INTO TABLE stocks;

DROP VIEW IF EXISTS joined_dataset;
CREATE OR REPLACE VIEW joined_dataset AS 
SELECT p.ticker AS ticker, volume, close, YEAR(ticker_date) AS ticker_year, sector
FROM prices p JOIN stocks s ON p.ticker=s.ticker
WHERE sector != 'N\A' AND ticker_date >= '2008-01-01' AND ticker_date <= '2018-12-31';

DROP VIEW IF EXISTS avg_volume_close;
CREATE OR REPLACE VIEW avg_volume_close AS
SELECT sector, ticker_year, AVG(volume) AS avg_volume, AVG(close) AS avg_close
FROM joined_dataset
GROUP BY sector, ticker_year
ORDER BY sector, ticker_year;

DROP VIEW IF EXISTS min_date_year;
CREATE OR REPLACE VIEW min_date_year AS
SELECT ticker, min(ticker_date) AS min_date, YEAR(ticker_date) AS ticker_year
FROM prices 
WHERE ticker_date >= '2008-01-01' AND ticker_date <= '2018-12-31'
GROUP BY ticker, YEAR(ticker_date);

DROP VIEW IF EXISTS max_date_year;
CREATE OR REPLACE VIEW max_date_year AS
SELECT ticker, max(ticker_date) AS max_date, YEAR(ticker_date) AS ticker_year
FROM prices 
WHERE ticker_date >= '2008-01-01' AND ticker_date <= '2018-12-31'
GROUP BY ticker, YEAR(ticker_date);

DROP VIEW IF EXISTS min_close_year;
CREATE OR REPLACE VIEW min_close_year AS
SELECT p.ticker AS ticker, ticker_year, close
FROM prices p JOIN min_date_year m ON p.ticker=m.ticker AND p.ticker_date=m.min_date;

DROP VIEW IF EXISTS max_close_year;
CREATE OR REPLACE VIEW max_close_year AS
SELECT p.ticker AS ticker, ticker_year, close
FROM prices p JOIN max_date_year m ON p.ticker=m.ticker AND p.ticker_date=m.max_date;

DROP VIEW IF EXISTS ticker_variation;
CREATE OR REPLACE VIEW ticker_variation AS
SELECT  mi.ticker AS ticker, mi.ticker_year as ticker_year, ROUND(((ma.close/mi.close) *100) - 100) AS variation
FROM min_close_year mi JOIN max_close_year ma ON mi.ticker=ma.ticker AND mi.ticker_year=ma.ticker_year;

DROP VIEW IF EXISTS sector_variation;
CREATE OR REPLACE VIEW sector_variation AS
SELECT sector, j.ticker_year AS sector_year, AVG(t.variation) AS variation
FROM joined_dataset j JOIN ticker_variation t ON j.ticker= t.ticker AND j.ticker_year=t.ticker_year
GROUP BY sector, j.ticker_year;

DROP VIEW IF EXISTS job2_result;
CREATE OR REPLACE VIEW job2_result AS
SELECT a.sector, a.ticker_year AS sector_year, a.avg_volume, s.variation, a.avg_close
FROM avg_volume_close a JOIN sector_variation s ON a.sector=s.sector AND a.ticker_year=s.sector_year;


INSERT OVERWRITE LOCAL DIRECTORY 'output/job2/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM job2_result ORDER BY sector, sector_year;