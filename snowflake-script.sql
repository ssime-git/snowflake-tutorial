-- Création de la table TRIPS pour ingestion des données
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

-- create extenal stage
/* Stage that specifies the location of our external bucket.
Rrecommandation: To prevent data egress/transfer costs in the future, 
you should select a staging location from the same cloud provider and region as your Snowflake account*/

-- A. Create a stage in the database tab

-- B. list the content of the stage (AKA the location)
list @citibike_trips;

-- Create a file format
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';
  
--Verify file format is created
show file formats in database citibike;

-- Part 5: Loading Data
-- 1. Change the size of the compute (Admin > compute: X-small to Small)

-- 2. Load the data: copy into trips from the location = @citibike_trips with the file_format=csv PATTERN = '.*csv.*' ;
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;
-- took up to 44s (34s execution)

-- 2.bis Load the data with a bigger warehouse
-- 2.bis1 Truncate everything
truncate table trips;

-- 2.bis2 Verify if the table is clear
select * from trips limit 10;

-- 2.bis3 change the warehouse size
--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

-- 3. copy file again:
copy into trips from @citibike_trips
file_format=CSV;
-- Took 15s

-- Part 6: Working with Queries, the Results Cache, & Cloning
-- 0. create an analytics warehouse
-- 1. change teh compute warehouse to Analytics

-- query
select * from trips limit 20;

-- the number of trips, average trip duration, and average trip distance: the first time it took 6s
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Result cache: By re executing the same query the treatmen should be quicker. the second time it took 71ms. The results are stored in cache for 24hours

-- which months are the busiest: 51ms
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- Clone a table
/*Snowflake allows you to create clones, also known as "zero-copy clones" (the underlying data is not copied) of tables, schemas, and databases in seconds. When a clone is created, Snowflake takes a snapshot of data present in the source object and makes it available to the cloned object. The cloned object is writable and independent of the clone source. Therefore, changes made to either the source object or the clone object are not included in the other.*/

/*A popular use case for zero-copy cloning is to clone a production environment for use by Development & Testing teams to test and experiment without adversely impacting the production environment and eliminating the need to set up and manage two separate environments.*/
create table trips_dev clone trips;

-- Part 7: Working with Semi-Structured Data, Views, & Joins
-- create a new Database weather
create database weather;

-- select role, compute warehouse, database and schema
use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;

-- 1. create JSON to hold semi-structured json data
create table json_weather_data (v variant);

-- 2. create new stage to ingest JSON data (AKA the location)
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

-- see the content of the bucket
list @nyc_weather;

-- 3. ingest the data
/*Note that you can specify a FILE FORMAT object inline in the command. In the previous section where we loaded structured data in CSV format, we had to define a file format to support the CSV structure. Because the JSON data here is well-formed, we are able to simply specify the JSON type and use all the default settings*/
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

-- 4. look at the data
select * from json_weather_data limit 10;

-- 5. create a view of the weather data:
/*The 72502 value for station_id corresponds to Newark Airport, the closest station that has weather conditions for the whole period*/

/* SQL dot notation v:temp is used in this command to pull out values at lower levels within the JSON object hierarchy. This allows us to treat each field as if it were a column in a relational table.*/

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

-- see the content of the view:
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- check citibike
select * from citibike.public.trips limit 5;

-- 6. Use a Join Operation to Correlate Against Data Sets
select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips -- has be fully qualified since we are suppose to be in weather DB
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null -- in weather
group by 1 order by 2 desc;


-- PART 8: Using Time Travel
/* The default window is 24 hours and, if you are using Snowflake Enterprise Edition, can be increased up to 90 days*/
-- 1. Accidently drop the jsn_weather_data
drop table json_weather_data;

-- check that everything was dopped
select * from json_weather_data limit 10;

-- undrop a table
undrop table json_weather_data;

--verify table is undropped
select * from json_weather_data_view limit 10;

-- 2. Roll Back a Table
-- select role, compute, database and schema
use role sysadmin;

use warehouse compute_wh;

use database citibike;

use schema public;

-- create a mistake
update trips set start_station_name = 'oops';

-- check the result
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

/*run a command to find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID.*/
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

-- Use Time Travel to recreate the table with the correct station names
create or replace table trips as
(select * from trips before (statement => $query_id));

-- check the result
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

// PART 9: Working with Roles, Account Admin, & Account Usage

-- 1. Change the role to account admin
use role accountadmin;

/* If you try to perform this operation while in a role such as SYSADMIN, it would fail due to insufficient privileges. By default (and design), the SYSADMIN role cannot create new roles or users.*/

-- 2. create a role and link the role to the username
create role junior_dba;

grant role junior_dba to user SEBASTIEN;

-- 3. Change your worksheet context to the new JUNIOR_DBA role
use role junior_dba;

-- 4. grant access to warehouse (compute)
use role accountadmin;

grant usage on warehouse compute_wh to role junior_dba;

-- 5. swith back to junior_dba role
use role junior_dba;

use warehouse compute_wh;

-- 6. grant access to DB
use role accountadmin;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;

use role junior_dba; -- swuth back