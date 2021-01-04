CREATE DATABASE lpg_dw;

CREATE SCHEMA IF NOT EXISTS dw_raw;

CREATE SCHEMA IF NOT EXISTS dw_bi;



CREATE TABLE IF NOT EXISTS dw_raw.gtrends_interest_over_time (
trend_timestamp TIMESTAMP,
elon_musk INT,
joe_biden INT,
taylor_swift INT,
godzilla INT,
black_panther INT,
ispartial BOOLEAN,
data_loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);


CREATE TABLE IF NOT EXISTS dw_raw.gtrends_interest_by_region (
country VARCHAR(150),
elon_musk INT,
joe_biden INT,
taylor_swift INT,
godzilla INT,
black_panther INT,
data_loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);


--dimension tables 
CREATE TABLE IF NOT EXISTS dw_bi.d_keyword(
k_id INT IDENTITY (1,1) PRIMARY KEY,
k_name VARCHAR(200)
);

INSERT INTO  dw_bi.d_keyword (k_name) VALUES('Elon Musk');
INSERT INTO  dw_bi.d_keyword (k_name) VALUES('Joe Biden');
INSERT INTO  dw_bi.d_keyword (k_name) VALUES('Taylor Swift');
INSERT INTO  dw_bi.d_keyword (k_name) VALUES('Godzilla');
INSERT INTO  dw_bi.d_keyword (k_name) VALUES('Black Panther');



CREATE TABLE IF NOT EXISTS dw_bi.d_country(
country_id INT IDENTITY (1,1) PRIMARY KEY, --can use country code instead of identity 
country_name VARCHAR(200)
);

--fact tables 
CREATE TABLE IF NOT EXISTS dw_bi.f_gtrends_interest_over_time (
trend_timestamp TIMESTAMP,
k_id INT,
no_of_searches INT,
data_loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
CONSTRAINT k_id_fk FOREIGN KEY (k_id) REFERENCES dw_bi.d_keyword );



CREATE TABLE IF NOT EXISTS dw_bi.f_gtrends_interest_by_region (
trend_timestamp TIMESTAMP,
country_name VARCHAR(200), -- we can use the d_country - country_id instead of name
k_id INT,
no_of_searches INT,
data_loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT k_id_fk FOREIGN KEY (k_id) REFERENCES dw_bi.d_keyword );



--Load into the fact tables

INSERT INTO dw_bi.f_gtrends_interest_over_time 
WITH cte_kw AS (

	SELECT trend_timestamp, 'Elon Musk' AS kw_name, elon_musk as no_of_searches FROM dw_raw.gtrends_interest_over_time

	UNION ALL 

	SELECT trend_timestamp, 'Joe Biden' AS kw_name, joe_biden as no_of_searches FROM dw_raw.gtrends_interest_over_time

	UNION ALL 


	SELECT trend_timestamp, 'Taylor Swift' AS kw_name, taylor_swift as no_of_searches FROM dw_raw.gtrends_interest_over_time

	UNION ALL 


	SELECT trend_timestamp, 'Godzilla' AS kw_name, godzilla as no_of_searches FROM dw_raw.gtrends_interest_over_time

	UNION ALL


	SELECT trend_timestamp, 'Black Panther' AS kw_name, black_panther as no_of_searches FROM dw_raw.gtrends_interest_over_time
)

SELECT 	trend_timestamp,
		k.k_id,
		no_of_searches

FROM cte_kw c JOIN dw_bi.d_keyword k ON k.k_name=c.kw_name;



INSERT INTO dw_bi.f_gtrends_interest_by_region 
WITH cte_kw AS (

	SELECT data_refreshed_timestamp, country,'Elon Musk' AS kw_name, elon_musk as no_of_searches FROM dw_raw.gtrends_interest_by_region

	UNION ALL 

	SELECT data_refreshed_timestamp, country, 'Joe Biden' AS kw_name, joe_biden as no_of_searches FROM dw_raw.gtrends_interest_by_region

	UNION ALL 


	SELECT data_refreshed_timestamp, country, 'Taylor Swift' AS kw_name, taylor_swift as no_of_searches FROM dw_raw.gtrends_interest_by_region

	UNION ALL 


	SELECT data_refreshed_timestamp, country, 'Godzilla' AS kw_name, godzilla as no_of_searches FROM dw_raw.gtrends_interest_by_region

	UNION ALL


	SELECT data_refreshed_timestamp, country, 'Black Panther' AS kw_name, black_panther as no_of_searches FROM dw_raw.gtrends_interest_by_region
)

SELECT 	data_refreshed_timestamp, -- we can bring in a date column from python instead of using this column as this may result in confusion if the files are loaded in a different day ( google trends extract didn't have date )
		country,
		k.k_id,
		no_of_searches

FROM cte_kw c JOIN dw_bi.d_keyword k ON k.k_name=c.kw_name;
