./produce.py | nc localhost 44444 1>/dev/null
flume-ng agent -n agent1 --conf conf -f /home/lgarswood/project/agent1.properties -Dflume.root.logger=INFO,console
HADOOP_CLASSPATH=/usr/share/cmf/cloudera-scm-telepub/jars/mysql-connector-java-5.1.15.jar sqoop export --connect 'jdbc:mysql://localhost/lgarswood' --table top_categories --export-dir /user/hive/warehouse/lg_top_categories --columns category -fields-terminated-by ','
JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk.x86_64 spark-shell --driver-class-path /usr/share/java/mysql-connector-java-5.1.34-bin.jar
JAVA_HOME=/usr/java/latest spark2-submit --jars /usr/share/cmf/common_jars/mysql-connector-java-5.1.15.jar --driver-class-path /usr/share/cmf/common_jars/mysql-connector-java-5.1.15.jar --driver-memory 600M --executor-memory 600M --class org.purchases.PurchaseTransformer --master yarn --deploy-mode cluster ~/project/spark/build/libs/spark-all.jar
CREATE EXTERNAL TABLE lg_purchases(
    product STRING,
    price DECIMAL(10, 2),
    date TIMESTAMP,
    category STRING,
    ip STRING
)
COMMENT "Lucas Garswood test product table"
PARTITIONED BY(event_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/lgarswood/events/';

alter table lg_purchases add partition (event_date='2018-11-29') location '2018/11/29';

CREATE TABLE lg_top_categories (category String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE lg_top_products (category String, product String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE lg_top_countries (country_name String, spending Decimal(10, 2))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

INSERT INTO lg_top_categories
SELECT category
FROM (
    SELECT *
    FROM (
        SELECT category, count(*) AS c
        FROM lg_purchases
        GROUP BY category
    ) cc
    ORDER BY c DESC
    LIMIT 10
) ccl;

INSERT INTO lg_top_products
SELECT category, product
FROM (
    SELECT *
    FROM (
        SELECT category, product, c,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY c DESC
        ) AS row_number
        FROM (
            SELECT category, product, COUNT(*) AS c
            FROM lg_purchases
            GROUP BY category, product
        ) cpc
    ) cpcr
    WHERE row_number <= 10
) cpcrl;

CREATE EXTERNAL TABLE lg_countries(
    geoname_id INT,
    locale_code STRING,
    continent_code STRING,
    continent_name STRING,
    country_iso_code STRING,
    country_name STRING,
    is_in_european_union INT
)
COMMENT "Lucas Garswood test country table"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/lgarswood/countries'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE lg_ips(
    network STRING,
    geoname_id INT,
    registered_country_geoname_id INT,
    represented_country_geoname_id INT,
    is_anonymous_proxy INT,
    is_satellite_provider INT
)
COMMENT "Lucas Garswood test ip table"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/lgarswood/ips'
tblproperties ("skip.header.line.count"="1");

INSERT INTO lg_top_countries
SELECT country_name, spending
FROM (
    SELECT lg_countries.country_name,
    SUM(CAST(purchases.price * 100 AS INT)) / 100 AS spending
    FROM (
        SELECT mask, registered_country_geoname_id,
        CAST((CAST(network_ip_contents[0] AS BIGINT) * POW(256, 3) + CAST(network_ip_contents[1] AS BIGINT) * POW(256, 2) + CAST(network_ip_contents[2] AS BIGINT) * 256 + CAST(network_ip_contents[3] AS BIGINT)) AS BIGINT) AS network_ip_int
        FROM (
          SELECT registered_country_geoname_id,
          CAST((POW(2, 32) - POW(2, 32 - CAST(ip_mask[1] AS INT))) AS BIGINT) AS mask,
          SPLIT(ip_mask[0], '[\.]') AS network_ip_contents
          FROM (
              SELECT registered_country_geoname_id, SPLIT(network, '[/]') AS ip_mask
              FROM lg_ips
          ) ip_1
        ) ip_2
    ) ips
    JOIN (
      SELECT price,
      CAST((CAST(ip_contents[0] AS BIGINT) * POW(256, 3) + CAST(ip_contents[1] AS BIGINT) * POW(256, 2) + CAST(ip_contents[2] AS BIGINT) * 256 + CAST(ip_contents[3] AS BIGINT)) AS BIGINT) AS ip_int
      FROM (
        SELECT price, SPLIT(ip, '[\.]') AS ip_contents
        FROM lg_purchases
      ) purchase_1
    ) purchases
    JOIN lg_countries
    ON ((purchases.ip_int ^ ips.network_ip_int) & ips.mask) = 0 AND ips.registered_country_geoname_id = lg_countries.geoname_id
    GROUP BY lg_countries.country_name
) cs
ORDER BY spending DESC
LIMIT 10;
