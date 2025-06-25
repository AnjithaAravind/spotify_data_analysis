CREATE WAREHOUSE spotify_ware;
USE WAREHOUSE spotify_ware;

CREATE DATABASE spotify_base;
USE DATABASE spotify_base;

CREATE SCHEMA spotify_sch;
USE SCHEMA spotify_sch;


CREATE STORAGE INTEGRATION s3_spotifysnowfalke
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/fake-placeholder-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://newanjibucket/transformed_data/');

DESC INTEGRATION s3_spotifysnowfalke;

CREATE OR REPLACE STAGE spotify_stage
  URL = 's3://newanjibucket/transformed_data/'
  STORAGE_INTEGRATION = s3_spotifysnowfalke
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

CREATE OR REPLACE TABLE spotify_data (
  Source STRING,
  Index_col STRING,
  ID STRING,
  Name STRING,
  URL STRING
);

CREATE OR REPLACE FILE FORMAT spotify_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ',';

COPY INTO spotify_data
FROM @spotify_stage
FILES = ('final_clean_spotify_data.csv')
ON_ERROR = 'CONTINUE';

ALTER STORAGE INTEGRATION s3_spotifysnowfalke
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::659376306968:role/spotifysnowflake';

SELECT * FROM spotify_data;


