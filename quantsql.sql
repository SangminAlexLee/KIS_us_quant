use stock_db;

select * from kis_token;

delete from kis_token ;

commit;

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;

-- delete from holding_stock_details where pdno ='071050';

select * from holding_stock_details;

SELECT *
        FROM kr_stock_price
        WHERE code = '005490' and  Date >= str_to_date('20241201','%Y%m%d');

CREATE INDEX code_date ON kr_stock_price (code(6), date);
        
SELECT code, date, close
FROM kr_stock_price
WHERE code = '005490' 
AND Date BETWEEN STR_TO_DATE('20241201', '%Y%m%d') AND STR_TO_DATE('20241227', '%Y%m%d')
;    

select * from kr_stock_price where date > '2024-12-01';

SET SQL_SAFE_UPDATES = 0;
SET SQL_SAFE_UPDATES = 1;

-- delete from holding_stock_details where pdno ='071050';

-- drop table holding_stock_details; 

select * From holding_stock_details;

commit;

select * from kr_stock_price where 1=1
and date > '2024-07-25';

## drop table model_return_5d;

select * from model_return_5d;

select * from model_return_5d;

select count(1) from (select distinct code from kr_stock_returns4 where 1=1 ) a
;

select * from model_return_5d_sigmoid;

select * from model_return_5d_rolling;

drop table model_return_5d_sigmoid;

select * from kr_stock_mkt_cap where 1=1
and date > '2024-07-23'; 

select count(1) from (select distinct code from kr_stock_returns_60d where 1=1 ) a
;

select * From strategy_1;

select * from kis_token;

 -- drop table kis_token;

delete from kis_token where token is not null;

select * from kr_stock_returns4 where 1=1
and code = '006040'
and end_date > '2024-09-01';

select * from krx_list where 1=1 
and name like '%삼성전기%'
;

select market, count(1) from (select b.market, a.* from kr_stock_returns4 a, krx_list b where 1=1
and a.code = b.code ) c
group by market 
;

select market, count(1) from (select b.market, a.* from kr_stock_returns_20d a, krx_list b where 1=1
and a.code = b.code ) c
group by market 
;




select 105287 + 151284 from dual;

1044418

 and end_date < '2019-09-23'
 -- and 'return' > 0
;

472150

472150

select count(1) from (select distinct code from kr_stock_returns4) a;

select count(1) from (select distinct code from kr_stock_returns_20d) a;

select * from kr_stock_10y_price where 1=1
and s_code = '000720'
and date > '2022-08-01';

select * from kr_stock_returns_20d where 1=1
and file_name = '000180_20161102_20_1.png';

select count(1) From kr_stock_returns4 a;

select b.market, count(1) 
From kr_stock_returns_60d a,krx_list b 
where a.code = b.code and b.market in ('KOSPI', 'KOSDAQ') group by b.market;

select * from krx_list;

select * from kr_stock_returns_60d;

select count(1) from (select distinct * From kr_stock_returns_20d ) a;