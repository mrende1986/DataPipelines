-- DDL to create `user_devices_cumulated` table that has:
create table user_devices_cumulated (
	user_id text,
	browser_type text,
	-- the list of dates in the past where the user was active
	device_activity_datelist date[],
	-- the current date for the user
	date date,
	primary key (user_id, browser_type, date)
)


-- Cumulative query to generate `device_activity_datelist` from `events`
INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist, date)
WITH yesterday AS (
  SELECT *
  FROM user_devices_cumulated
  WHERE date = DATE '2023-01-30'
),
today AS (
  SELECT
    CAST(e.user_id AS TEXT) AS user_id,
    d.browser_type,
    DATE(e.event_time) AS date_active
  FROM events e
  JOIN devices d ON e.device_id = d.device_id
  WHERE DATE(e.event_time) = DATE '2023-01-31'
    AND e.user_id IS NOT NULL
  GROUP BY e.user_id, d.browser_type, DATE(e.event_time)
)

SELECT
  COALESCE(t.user_id, y.user_id) AS user_id,
  COALESCE(t.browser_type, y.browser_type) AS browser_type,
  CASE 
    WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
    WHEN t.date_active IS NULL THEN y.device_activity_datelist
    ELSE y.device_activity_datelist || ARRAY[t.date_active]
  END AS device_activity_datelist,
  COALESCE(t.date_active, y.date + INTERVAL '1 day')::DATE AS date

FROM today t
FULL OUTER JOIN yesterday y
  ON t.user_id = y.user_id AND t.browser_type = y.browser_type

ON CONFLICT (user_id, browser_type, date) DO UPDATE
SET device_activity_datelist = user_devices_cumulated.device_activity_datelist || EXCLUDED.device_activity_datelist;

select * from user_devices_cumulated;

-- Convert `device_activity_datelist` column into a `datelist_int` column 

with users as (
	select * 
	from user_devices_cumulated
	where date = '2023-01-31'
),
series as (
	SELECT generate_series(DATE '2023-01-02', DATE '2023-01-31', INTERVAL '1 day')::DATE AS series_date
)

,place_holder_ints as (

	select 
		case
			when series.series_date = any(users.device_activity_datelist)
				then power(2, 32 - (users.date - series.series_date))
			else 0
		end as placeholder_int_value,
		*
	from users
	cross join series
)

select 
	user_id,
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),
	bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) as dim_is_monthly_active
from place_holder_ints
group by user_id;


-- DDL for `hosts_cumulated` table 

create table hosts_cumulated(
	host text,
	host_activity_datelist date,
	metric_name text,
	metric_array real[],
	primary key (host, host_activity_datelist, metric_name)
)

-- Incremental query to generate `host_activity_datelist`
insert into hosts_cumulated
with daily_aggregate as (
	select 
		host,
		date(event_time) as date,
		count(1) as num_site_hits
	from events
	where date(event_time) = date('2023-01-03')
	and host is not null
	group by host, date(event_time)
),

yesterday_array as (
	select *
	from hosts_cumulated
	where host_activity_datelist = date('2023-01-02')
)

select
	coalesce(da.host, ya.host) as host,
	coalesce(ya.host_activity_datelist, da.date) as host_activity_datelist,
	'host_activity' as metric_name,
	case
		when ya.metric_array is not null
			then ya.metric_array || array[coalesce(da.num_site_hits, 0)]
		when ya.metric_array is null
			then array_fill(0, array[coalesce(date - date,0)])
				|| array[coalesce(da.num_site_hits,0)]
	end as metric_array
	

	from daily_aggregate da
	full outer join yesterday_array ya
		on	da.host = ya.host
	on conflict (host, host_activity_datelist, metric_name)
	do update set metric_array = excluded.metric_array;


	
-- DDL for `host_activity_reduced` table

CREATE TABLE host_activity_reduced (
	month_start DATE,
	host TEXT,
	daily_hits_array INTEGER[],
	daily_unique_visitors INTEGER[],
	date date,
	PRIMARY KEY (month_start, host)
);

-- Incremental query that loads `host_activity_reduced`
INSERT INTO host_activity_reduced
with yesterday as (
	select *
	from host_activity_reduced
	WHERE date = DATE '2023-01-02')

, today AS (
  SELECT
    DATE_TRUNC('month', event_time::timestamp) AS month_start,
    host,
    DATE(event_time) AS event_day,
    COUNT(*) AS hits,
    COUNT(DISTINCT user_id) AS unique_visitors,
    date '2023-01-03' as date
  FROM events
  where date(event_time) = DATE '2023-01-03'
  GROUP BY month_start, host, event_day
)

SELECT
	coalesce(t.month_start, y.month_start) as month_start,

	COALESCE(t.host, y.host) AS host,
   
	CASE 
    	WHEN y.daily_hits_array IS NULL 
    		THEN ARRAY[t.hits]
    	WHEN t.hits IS NULL 
    		THEN y.daily_hits_array
    	ELSE y.daily_hits_array || ARRAY[t.hits]
  	END AS daily_hits_array,
  	
  	CASE 
    	WHEN y.daily_unique_visitors IS NULL 
    		THEN ARRAY[t.unique_visitors]
    	WHEN t.unique_visitors IS NULL 
    		THEN y.daily_unique_visitors
    	ELSE y.daily_unique_visitors || ARRAY[t.unique_visitors]
  	END AS daily_unique_visitors,
  
  COALESCE(t.date, y.date + INTERVAL '1 day')::DATE AS date

FROM today t
FULL OUTER JOIN yesterday y
  ON t.host = y.host AND t.date = date(y.date)

ON CONFLICT (month_start, host) DO UPDATE
SET
  daily_hits_array = EXCLUDED.daily_hits_array,
  daily_unique_visitors = EXCLUDED.daily_unique_visitors;

select * from host_activity_reduced;
