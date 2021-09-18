DROP TABLE if exists calendar;

CREATE TABLE calendar (
  day_id DATE NOT NULL PRIMARY KEY,
  year SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL, 
  day_of_week SMALLINT NOT NULL, 
  day_of_year SMALLINT NOT NULL, 
  week_of_year SMALLINT NOT NULL,
  CONSTRAINT con_month CHECK (month >= 1 AND month <= 31),
  CONSTRAINT con_day_of_year CHECK (day_of_year >= 1 AND day_of_year <= 366),
  CONSTRAINT con_week_of_year CHECK (week_of_year >= 1 AND week_of_year <= 53)
);

INSERT INTO calendar (day_id, year, month, day, quarter, day_of_week, day_of_year, week_of_year)
(SELECT ts, 
  EXTRACT(YEAR FROM ts),
  EXTRACT(MONTH FROM ts),
  EXTRACT(DAY FROM ts),
  EXTRACT(QUARTER FROM ts),
  EXTRACT(DOW FROM ts),
  EXTRACT(DOY FROM ts),
  EXTRACT(WEEK FROM ts)
  FROM generate_series('2021-01-01'::timestamp, '2021-12-31', '1day'::interval) AS t(ts));
