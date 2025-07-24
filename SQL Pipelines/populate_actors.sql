CREATE TYPE films AS (
 					film_name TEXT,
 					votes REAL,
 					rating REAl,
 					filmid TEXT
 				);

CREATE TYPE quality_class AS ENUM ('star','good','average','bad');


CREATE TABLE actors (
	actor TEXT,
	actorid text,
	current_year real,
	films films[],
	quality_class quality_class,
	is_active boolean,
	PRIMARY KEY(actor, actorid)
	);


-- incremental populate query to populate the actors table one year at a time.
INSERT INTO actors (
  actor,
  actorid,
  current_year,
  films,
  quality_class,
  is_active
)
WITH
yesterday AS (
  SELECT *
  FROM actors
  WHERE current_year = 1977
),
today AS (
  SELECT 
    actor,
    actorid,
    year,
    ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films,
    AVG(rating) AS avg_rating
  FROM actor_films
  WHERE year = 1978
  GROUP BY actor, actorid, year
)
SELECT 
  COALESCE(t.actor, y.actor) AS actor,
  COALESCE(t.actorid, y.actorid) AS actorid,
  COALESCE(t.year, y.current_year) AS current_year,
  CASE 
    WHEN y.films IS NULL THEN t.films
    WHEN t.films IS NOT NULL THEN y.films || t.films
    ELSE y.films
  END AS films,
  CASE
    WHEN t.avg_rating > 8 THEN 'star'
    WHEN t.avg_rating > 7 THEN 'good'
    WHEN t.avg_rating > 6 THEN 'average'
    WHEN t.avg_rating IS NOT NULL THEN 'bad'
    ELSE y.quality_class
  END::quality_class AS quality_class,
  (t.films IS NOT NULL) AS is_active
FROM today t
FULL OUTER JOIN yesterday y
  ON t.actor = y.actor AND t.actorid = y.actorid

ON CONFLICT (actor, actorid) DO UPDATE SET
  current_year = EXCLUDED.current_year,
  films = actors.films || EXCLUDED.films,
  quality_class = EXCLUDED.quality_class,
  is_active = EXCLUDED.is_active;



-- create table
CREATE TABLE actors_history_scd (
		actor TEXT,
		quality_class quality_class,
		is_active BOOLEAN,
		start_film_year INTEGER,
		end_film_year INTEGER,
		current_year INTEGER,
		PRIMARY KEY(actor, current_year)
);


-- backfill query that can populate the entire `actors_history_scd` table in a single query.

WITH streak_started AS (
    SELECT actor,
           current_year,
           quality_class,
           LAG(quality_class, 1) over (PARTITION BY actor ORDER BY current_year) <> quality_class
        OR LAG(quality_class, 1) over (PARTITION BY actor ORDER BY current_year) IS null
        or LAG(is_active, 1) over (PARTITION BY actor ORDER BY current_year) <> is_active
        or LAG(is_active, 1) over (PARTITION BY actor ORDER BY current_year) IS null
               AS did_change
    FROM actors
),
     streak_identified AS (
         SELECT
            actor,
                quality_class,
                current_year,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            quality_class,
            streak_identifier,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3
     )

     SELECT actor, quality_class, start_date, end_date, streak_identifier
     FROM aggregated
	 ORDER BY 1

-- incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table
CREATE TYPE scd_film_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_film_year INTEGER,
                    end_film_year INTEGER
                        )
	 
	 
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1971
    AND end_film_year = 1971
)
,
     historical_scd AS (
        SELECT
            actor,
               quality_class,
               is_active,
               start_film_year,
               end_film_year
        FROM actors_history_scd
        WHERE current_year = 1971
        AND end_film_year < 1971
     ),
     this_year_data AS (
         SELECT * FROM actors
         WHERE current_year = 1971
     )
     ,
     unchanged_records AS (
         SELECT
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_film_year,
                ts.current_year as end_year
        FROM this_year_data ts
        JOIN last_year_scd ls
        ON ls.actor = ts.actor
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     )
     ,
     changed_records AS (
        SELECT
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_film_year,
                        ls.end_film_year

                        )::scd_film_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::scd_film_type
                ]) as records
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actor = ts.actor
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     )
     ,
     unnested_changed_records AS (

         SELECT actor,
                (records::scd_film_type).quality_class,
                (records::scd_film_type).is_active,
                (records::scd_film_type).start_film_year,
                (records::scd_film_type).end_film_year
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.current_year AS start_film_year,
                ts.current_year AS end_film_year
         FROM this_year_data ts
         LEFT JOIN last_year_scd ls
             ON ts.actor = ls.actor
         WHERE ls.actor IS NULL

     )
     


SELECT *, 1971 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
                  ) a
                  