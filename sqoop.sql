CREATE DATABASE lgarswood;
USE lgarswood;

CREATE TABLE top_categories (
   id INT NOT NULL AUTO_INCREMENT,
   category VARCHAR(20),
   PRIMARY KEY(id));

CREATE TABLE top_products (
   id INT NOT NULL AUTO_INCREMENT,
   category VARCHAR(20),
   product VARCHAR(30),
   PRIMARY KEY(id));

CREATE TABLE top_countries (
  id INT NOT NULL AUTO_INCREMENT,
  country_name VARCHAR(20),
  spending DECIMAL(10, 2),
  PRIMARY KEY(id));

----------- SPARK EQUIVALENTS ------------------

  CREATE TABLE top_categories_spark (
     id INT NOT NULL AUTO_INCREMENT,
     category VARCHAR(20),
     PRIMARY KEY(id));

  CREATE TABLE top_products_spark (
     id INT NOT NULL AUTO_INCREMENT,
     category VARCHAR(20),
     product VARCHAR(30),
     PRIMARY KEY(id));

  CREATE TABLE top_countries_spark (
    id INT NOT NULL AUTO_INCREMENT,
    country_name VARCHAR(20),
    spending DECIMAL(10, 2),
    PRIMARY KEY(id));

-------------------------------------------------------

sqoop export \
--connect jdbc:mysql://10.0.0.21/lgarswood \
--table top_categories \
--export-dir /user/hive/warehouse/lg_top_categories \
--columns category \
--input-fields-terminated-by ',' \
--lines-terminated-b

sqoop export \
--connect jdbc:mysql://10.0.0.21/lgarswood \
--username root \
--password cloudera \
--table top_products \
--export-dir /user/hive/warehouse/lg_top_products \
--columns category,product \
--input-fields-terminated-by ','

sqoop export \
--connect jdbc:mysql://10.0.0.21/lgarswood \
--username root \
--password cloudera \
--table top_countries \
--export-dir /user/hive/warehouse/lg_top_countries \
--columns country_name,spending \
--input-fields-terminated-by ','
