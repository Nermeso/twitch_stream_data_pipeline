CREATE TABLE "day_dates" (
  "day_date_id" varchar(8) PRIMARY KEY,
  "the_date" date,
  "date_MMDDYYYY" varchar(10),
  "day_of_week" varchar,
  "month" char(2),
  "day" char(2),
  "year" char(4),
  "month_name" varchar,
  "month_abbrev" varchar(3),
  "year_YY" char(2)
);

CREATE TABLE "time_of_day" (
  "time_of_day_id" char(4) PRIMARY KEY,
  "time_24h" time,
  "time_12h" char(8),
  "hour" int,
  "minute" int,
  "AM_PM" char(2),
  "part_of_day" varchar
);

CREATE TABLE "users" (
  "user_id" varchar PRIMARY KEY,
  "user_name" varchar,
  "login_name" varchar,
  "broadcaster_type" varchar
);

CREATE TABLE "categories" (
  "category_id" varchar PRIMARY KEY,
  "igdb_id" varchar,
  "category_name" varchar
);

CREATE TABLE "genres" (
  "genre_id" varchar PRIMARY KEY,
  "genre_name" varchar
);

CREATE TABLE "genre_bridge" (
  "category_id" varchar REFERENCES categories(category_id),
  "genre_id" varchar REFERENCES genres(genre_id)
);

CREATE TABLE "game_modes" (
  "game_mode_id" varchar PRIMARY KEY,
  "game_mode_name" varchar
);

CREATE TABLE "game_mode_bridge" (
  "category_id" varchar REFERENCES categories(category_id),
  "game_mode_id" varchar REFERENCES game_modes(game_mode_id)
);

CREATE TABLE "languages" (
  "language_id" varchar PRIMARY KEY,
  "language_name" varchar
);

CREATE TABLE "streams" (
  "stream_id" varchar,
  "date_day_id" varchar(8) REFERENCES day_dates(day_date_id),
  "time_of_day_id" varchar(4) REFERENCES time_of_day(time_of_day_id),
  "user_id" varchar NOT NULL,
  "category_id" varchar REFERENCES categories(category_id),
  "language_id" varchar NOT NULL,
  "viewer_count" int,
  PRIMARY KEY (stream_id, date_day_id, time_of_day_id)
);
