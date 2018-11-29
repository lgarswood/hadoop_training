package org.purchases

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, count, rank, udf, lit}
import org.apache.spark.sql.types.{IntegerType, DecimalType}

// Had some trouble with decimal precision, so treating prices as integers 100x their value
case class Purchase(product: String, priceX100: Long, category: String, ip: Long)
case class Country(geonameId: Int, countryName: String)
case class CountryNetwork(geonameId: Int, ip: Long, mask: Long)

object PurchaseTransformer {
  /* test:
    val purchases = sc parallelize Seq(Purchase("product1", 10.2.toFloat, new java.sql.Timestamp(0), "category1", "10.1.2.3"), Purchase("product2", 10.3.toFloat, new java.sql.Timestamp(0), "category1", "10.1.3.3"), Purchase("product3", 1.02.toFloat, new java.sql.Timestamp(0), "category2", "10.2.4.2")) toDS
    val countries = sc parallelize Seq(Country(1, "abc", "def", "ghi", "jkl", "Jamaica", 0), Country(2, "abc", "def", "ghi", "jkl", "Canada", 0), Country(3, "abc", "def", "ghi", "jkl", "Russia", 0)) toDS
    val countryNetworks = sc parallelize Seq(CountryNetwork("10.1.2.1/24", 1, 0, 0, 0, 0), CountryNetwork("10.1.3.1/24", 2, 0, 0, 0, 0), CountryNetwork("10.2.4.1/24", 3, 0, 0, 0, 0)) toDS
  */

  def main(args: Array[String]) = {
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val jdbcUrl = "jdbc:mysql://10.0.0.21:3306/lgarswood"
    val properties = new java.util.Properties()
    //properties.put("user", "root")
    //properties.put("password", "cloudera")

    val ip = (x: String) => x.split("\\.").zipWithIndex.map(x => x._1.toInt * BigInt(256).pow(3 - x._2)).reduce(_ + _).toLong
    val mask = (m: Int) => (BigInt(2).pow(32) - BigInt(2).pow(32 - m)).toLong

    val purchases = spark.read.csv("/user/lgarswood/events/*/*/*/*")
      .map((r: org.apache.spark.sql.Row) => Purchase(r.getString(0), (r.getString(1).toFloat * 100).toLong, r.getString(3), ip(r.getString(4))))
      .cache()

    val topCategories = purchases
      .groupBy($"category")
      .agg(count("*") as "count")
      .orderBy($"count" desc)
      .select($"category")
      .limit(10)

    topCategories
      .write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "top_categories_spark", properties)

    val topProducts = purchases
      .groupBy($"category", $"product")
      .agg(count("*") as "count")
      .withColumn("rank",
        (rank() over Window
          .partitionBy($"category")
          .orderBy($"count" desc)
        )
      )
      .where($"rank" <= 10)
      .select($"category", $"product")

    topProducts
      .write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "top_products_spark", properties)

    val ipResidue = udf((x: Long, y: Long, z: Long) => (x ^ y) & z)
    val countries = spark.read.option("header","true").csv("/user/lgarswood/countries/*")
      .select($"geoname_id" cast IntegerType as "geonameId", $"country_name" as "countryName")
      .as[Country]
    val countryNetworks = spark.read.option("header","true").csv("/user/lgarswood/ips/*")
      .filter(_.getString(1) != null)
      .map((r: org.apache.spark.sql.Row) => {
        val networkParts = r getString 0 split "/"
        CountryNetwork(r.getString(1).toInt, ip(networkParts(0)), mask(networkParts(1).toInt))
      })
    val countryIps = countries
      .joinWith(countryNetworks, countries("geonameId") === countryNetworks("geonameId"))
      .select($"_1.countryName" as "countryName", $"_2.ip" as "ip", $"_2.mask" as "mask")
    val topCountries = countryIps
      .joinWith(purchases, ipResidue(purchases("ip"), countryIps("ip"), countryIps("mask")) === lit(0))
      .groupBy($"_1.countryName" as "country_name")
      .agg(sum($"_2.priceX100").cast(DecimalType(10, 2)) / 100 as "spending")
      .orderBy($"spending" desc)
      .limit(10)

    topCountries
      .write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "top_countries_spark", properties)
  }
}
