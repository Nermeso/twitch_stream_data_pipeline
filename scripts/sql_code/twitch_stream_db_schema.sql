CREATE TABLE "day_dates" (
  "day_date_id" varchar(8) PRIMARY KEY,
  "the_date" date,
  "date_MMDDYYYY" varchar(8),
  "day_of_week" varchar,
  "month" int,
  "day" int,
  "year" int,
  "month_name" varchar,
  "month_abbrev" varchar(3),
  "year_YY" int
);

CREATE TABLE "time_of_day" (
  "time_of_day_id" varchar(4) PRIMARY KEY,
  "time_24h" time,
  "time_12h" varchar(8),
  "hour" int,
  "minute" int,
  "AM_PM" varchar(2),
  "part_of_day" varchar
);

 CREATE TABLE "users" (
  "user_id" int PRIMARY KEY,
  "user_name" varchar,
  "login_name" varchar,
  "broadcaster_type" varchar
);

CREATE TABLE "categories" (
  "category_id" int PRIMARY KEY,
  "igdb_id" int,
  "category_name" varchar
);

CREATE TABLE "genres" (
  "genre_id" int PRIMARY KEY,
  "genre_name" varchar
);

CREATE TABLE "genre_bridge" (
  "category_id" int REFERENCES categories(category_id),
  "genre_id" int REFERENCES genres(genre_id)
);

CREATE TABLE "game_modes" (
  "game_mode_id" int PRIMARY KEY,
  "game_mode_name" varchar
);

CREATE TABLE "game_mode_bridge" (
  "category_id" int REFERENCES categories(category_id),
  "game_mode_id" int REFERENCES game_modes(game_mode_id)
);

CREATE TABLE "languages" (
  "language_id" varchar(2) PRIMARY KEY,
  "language_name" varchar
);

CREATE TABLE "streams" (
  "stream_id" int PRIMARY KEY,
  "date_day_id" varchar(8) REFERENCES day_dates(day_date_id),
  "time_of_day_id" varchar(4) REFERENCES time_of_day(time_of_day_id),
  "user_id" int REFERENCES users(user_id),
  "category_id" int REFERENCES categories(category_id),
  "language_id" varchar(2) REFERENCES languages(language_id),
  "viewer_count" int NOT NULL
);
