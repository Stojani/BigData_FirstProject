set hive.auto.convert.join = false;

CREATE TABLE IF NOT EXISTS prices (
                                    ticker STRING,
                                    open DOUBLE,
                                    close INT,
                                    adj_close DOUBLE,
                                    low DOUBLE,
                                    high DOUBLE, 
                                    volume DOUBLE, 
                                    ticker_date DATE)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' 
OVERWRITE INTO TABLE prices; 


DROP VIEW IF EXISTS single_fields_statistics;
CREATE OR REPLACE VIEW single_fields_statistics AS
SELECT p.ticker, min(p.close) AS min_price, max(p.close) AS max_price, avg(p.volume) AS avg_volume
FROM prices AS p
WHERE p.ticker_date BETWEEN Date('2008-01-01') AND Date('2018-12-31')
GROUP BY p.ticker;

DROP VIEW IF EXISTS first_last_date;
CREATE OR REPLACE VIEW first_last_date AS
SELECT p.ticker, min(p.ticker_date) AS first_date, max(p.ticker_date) AS last_date
FROM prices AS p
WHERE p.ticker_date BETWEEN Date('2008-01-01') AND Date('2018-12-31')
GROUP BY p.ticker;

DROP VIEW IF EXISTS first_close;
CREATE OR REPLACE VIEW first_close AS
SELECT p.ticker, fd.first_date, p.close
FROM first_last_date AS fd 
JOIN prices AS p ON (fd.ticker=p.ticker) AND (p.ticker_date=fd.first_date);

DROP VIEW IF EXISTS last_close;
CREATE OR REPLACE VIEW last_close AS
SELECT p.ticker, fd.last_date, p.close
FROM first_last_date AS fd 
JOIN prices AS p ON (fd.ticker=p.ticker) AND (p.ticker_date=fd.last_date);

DROP VIEW IF EXISTS ticker_variation;
CREATE OR REPLACE VIEW ticker_variation AS
SELECT fc.ticker, (((lc.close-fc.close)/fc.close) * 100) AS variation
FROM first_close AS fc
JOIN last_close AS lc ON lc.ticker=fc.ticker;

DROP VIEW IF EXISTS job1_result;
CREATE OR REPLACE VIEW job1_result AS
SELECT  sts.ticker, tv.variation, sts.min_price, sts.max_price, sts.avg_volume
FROM single_fields_statistics AS sts
JOIN ticker_variation AS tv ON tv.ticker=sts.ticker
ORDER BY tv.variation DESC;

INSERT OVERWRITE LOCAL DIRECTORY 'output/job1/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM job1_result LIMIT 10