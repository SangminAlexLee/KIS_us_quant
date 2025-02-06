use stock_db;

select * from kis_token ;

select * from holding_us_stock_details;

-- ETF Price 
SELECT * FROM us_stock_price where symbol in (select symbol from us_etf_list) ;

SELECT date max_date FROM us_stock_price GROUP BY date HAVING COUNT(*) >= 5800 ORDER BY date DESC LIMIT 1;

select count(1) from (select distinct symbol from us_stock_price where date >= '2025-01-22') a ;

SELECT * FROM us_stock_list where ins_date >= '2025-01-18';

SELECT * FROM us_stock_list WHERE ins_date = (SELECT MAX(ins_date) FROM us_stock_list) and symbol not in ( select symbol from us_etf_list );

SELECT * FROM us_stock_list WHERE ins_date = (SELECT ins_date max_date FROM us_stock_list where symbol not in ( select symbol from us_etf_list ) 
GROUP BY ins_date HAVING COUNT(*) >= 5800 ORDER BY ins_date DESC LIMIT 1);

SELECT * FROM us_stock_list WHERE ins_date = (SELECT MAX(ins_date) FROM us_stock_list);

SELECT date max_date FROM us_stock_price GROUP BY date HAVING COUNT(*) >= 5000 ORDER BY date DESC LIMIT 1;

select * from us_stock_price where date = '2025-01-21';

SELECT max(date) max_date FROM us_stock_price where symbol = 'SPY' ;

select max(date) from us_stock_price ;

alter table us_stock_price add primary key ( symbol(8) , date);

select date, count(1) from us_stock_price GROUP BY date HAVING COUNT(*) >= 2000;

select * from us_stock_price where 1=1 and symbol ='SPY'
order by date;

SELECT date max_date FROM us_stock_price GROUP BY date HAVING COUNT(*) >= 6000 ORDER BY date DESC LIMIT 1;

delete from us_stock_price where symbol in( 'ARKQ', 'SPY');
commit;

select * from us_etf_list;

# 주식 리스트 
select * from us_stock_list where 1=1 
and ins_date > '2025-01-20';

select * from us_stock_list where 1=1
and symbol like 'ARKQ';

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;
commit;


delete from us_stock_list where symbol = 'ARKQ';

# 주식 리스트 
select count(1) from us_stock_list where 1=1 
--and ins_date = '2025-01-09'
;

select distinct ins_date from us_stock_list where 1=1 ;

select * from us_snp500_list;

-- Create a table to manage holidays by year
CREATE TABLE holidays (
    id INT AUTO_INCREMENT PRIMARY KEY,       -- Unique identifier for each holiday
    year INT NOT NULL,                       -- Year of the holiday
    date DATE NOT NULL,                      -- Specific date of the holiday
    holiday_name VARCHAR(100) NOT NULL,      -- Name of the holiday
    country_code CHAR(2) NOT NULL,           -- ISO 3166-1 alpha-2 country code
    is_public_holiday BOOLEAN DEFAULT TRUE,  -- Indicates if the holiday is a public holiday (default is TRUE)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- Last update timestamp
);

-- Add an index to quickly search holidays by date and country code
CREATE INDEX idx_date_country ON holidays (date, country_code);

-- Example insertion of data
INSERT INTO holidays (year, date, holiday_name, country_code, is_public_holiday) VALUES
(2025, '2025-01-01', 'New Year\'s Day', 'US', TRUE),
(2025, '2025-01-20', 'Martin Luther King Jr. Day', 'US', TRUE),
(2025, '2025-02-17', 'Presidents\' Day', 'US', TRUE),
(2025, '2025-04-18', 'Good Friday', 'US', TRUE),
(2025, '2025-05-26', 'Memorial Day', 'US', TRUE),
(2025, '2025-06-19', 'Juneteenth National Independence Day', 'US', TRUE),
(2025, '2025-07-04', 'Independence Day', 'US', TRUE),
(2025, '2025-09-01', 'Labor Day', 'US', TRUE),
(2025, '2025-11-27', 'Thanksgiving Day', 'US', TRUE),
(2025, '2025-12-25', 'Christmas Day', 'US', TRUE);

commit;

select * from holidays;



