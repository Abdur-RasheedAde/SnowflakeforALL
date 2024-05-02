# SnowflakeforALL
// Create table
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

list @citibike_trips;

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

  --verify file format is created

show file formats in database citibike;

copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

truncate table trips;

--verify table is clear
select * from trips limit 10;

--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

copy into trips from @citibike_trips
file_format=CSV;

-- query data in the COMPUTE_WH
select * from trips limit 20;

-- query data in the ANALYTICS_WH
select * from trips limit 20;

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Running same query again is faster because of the catche ability
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- which months are the busiest?    
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

--Create a Clone of a Table
//Zero-Copy Cloning A massive benefit of zero-copy cloning is that the underlying data is not copied. Only the metadata and pointers to the underlying data change. Hence, clones are "zero-copy" and storage requirements are not doubled when the data is cloned. Most data warehouses cannot do this, but for Snowflake it is easy!

-- clone a table
create table trips_dev clone trips;

--Working with Semi-Structured Data, Views, & Joins
//SEMI-STRUCTURED DATA: Snowflake can easily load and query semi-structured data such as JSON, Parquet, or Avro without transformation. This is a key Snowflake feature because an increasing amount of business-relevant data being generated today is semi-structured, and many traditional data warehouses cannot easily load and query such data. Snowflake makes it easy!

--creating a new database
create database weather;

--set the worksheet context:
use role accountadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- create table JSON_WEATHER_DATA for loading the JSON data. 
// Note that Snowflake has a special column data type called VARIANT that allows storing the entire JSON object as a single row and eventually query the object directly.
create table json_weather_data (v variant);

--Create Another External Stage
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

--check the content of the list
list @nyc_weather;

--copy data from external stage into the just created json_weather_data table 
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

--check out the data
select * from json_weather_data limit 10;

--CREATE VIEWS
//Views & Materialized Views A view allows the result of a query to be accessed as if it were a table. Views can help present data to end users in a cleaner manner, limit what end users can view in a source table, and write more modular SQL. Snowflake also supports materialized views in which the query results are stored as though the results are a table. This allows faster access, but requires storage space. Materialized views can be created and queried if you are using Snowflake Enterprise Edition (or higher).

// create a view that will put structure onto the semi-structured data
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

--Verify the view with the following query:
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

--USING JOINS between 2 Tables
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

--Determine if there is any correlation between the number of bike rides and the weather by analyzing both ridership and weather data. 
--The number of trips is significantly higher when the weather is good!


//Using Time Travel
//Snowflake's powerful Time Travel feature enables accessing historical data, as well as the objects storing the data, at any point within a period of time. The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can be increased up to 90 days.

--Abiility to:
--Restoring data-related objects such as tables, schemas, and databases that may have been deleted.
--Duplicating and backing up data from key points in the past.
--Analyzing data usage and manipulation over specified periods of time.

--DROP table accidentally
drop table json_weather_data;

select * from json_weather_data limit 10;

--restore the table
undrop table json_weather_data;

--verify table is undropped
select * from json_weather_data limit 10;

//Roll Back a Table
//Let's roll back the TRIPS table in the CITIBIKE database to a previous state to fix an unintentional DML error that replaces all the station names in the table with the word "oops".


--switch worksheet to context:
use role accountadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

--replace all station names with oops
update trips set start_station_name = 'oops';

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

--find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

--Use Time Travel to recreate the table with the correct station names:
create or replace table trips as
(select * from trips before (statement => $query_id));

--verify that the station names have been restored:
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 12;

--WORKING WITH roles, ACCOUNT ADMIN AND ACCOUNT USAGE 
//Role-Based Access Control Snowflake offers very powerful and granular access control that dictates the objects and functionality a user can access, as well as the level of access they have. For more details.

--Create a New Role and Add a User
use role accountadmin;

--create a new role named JUNIOR_DBA and assign it to your Snowflake user
create role junior_dba;
grant role junior_dba to user DAAXRasheed;

use role junior_dba;

--grant junior_dba access to compute_wh
use role accountadmin;
grant usage on warehouse compute_wh to role junior_dba;

--switch to junior_dba
use role junior_dba;
use warehouse compute_wh;


--grant the JUNIOR_DBA the USAGE privilege required to view and use the CITIBIKE and WEATHER databases:

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

use role junior_dba;

--Sharing Data Securely & the Data Marketplace
//Snowflake enables data access between accounts through the secure data sharing features. Shares are created by data providers and imported by data consumers, either through their own Snowflake account or a provisioned Snowflake Reader account. The consumer can be an external entity or a different internal business unit that is required to have its own unique Snowflake account.

//With secure data sharing:
--There is only one copy of the data that lives in the data provider's account.
--Shared data is always live, real-time, and immediately available to consumers.
--Providers can establish revocable, fine-grained access to shares.
--Data sharing is simple and safe, especially compared to older data sharing methods, which were often manual and insecure, such as transferring large .csv files across the internet.
