-- ===============================================================
-- Title      : Medallion Architecture Database Setup (MySQL)
-- Description: Initializes Bronze, Silver, and Gold databases 
--              following the Medallion architecture pattern.
--              - Bronze: raw data
--              - Silver: cleaned/transformed data
--              - Gold  : analytics-ready data
-- ===============================================================

DROP DATABASE IF EXISTS Bronze;
CREATE DATABASE Bronze;

DROP DATABASE IF EXISTS Silver;
CREATE DATABASE Silver;

DROP DATABASE IF EXISTS Gold;
CREATE DATABASE Gold;
