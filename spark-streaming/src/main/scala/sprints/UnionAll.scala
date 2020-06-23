package sprints

import org.apache.spark.sql.SparkSession

class UnionAll {

  val spark = SparkSession.builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate

  val df2009 = spark.read.format("parquet")
    .option("header", "true")
    .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2009-json_corrigido.parquet\\*.parquet")
    .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
    .createOrReplaceTempView("ViewDf2009")

  val df2010 = spark.read.format("parquet")
    .option("header", "true")
    .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2010-json_corrigido.parquet\\*.parquet")
    .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
    .createOrReplaceTempView("ViewDf2010")

  val df2011 = spark.read.format("parquet")
    .option("header", "true")
    .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2011-json_corrigido.parquet\\*.parquet")
    .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
    .createOrReplaceTempView("ViewDf2011")

  val df2012 = spark.read.format("parquet")
    .option("header", "true")
    .load("src\\main\\resources\\data\\s3\\data-sample_data-nyctaxi-trips-2012-json_corrigido.parquet\\*.parquet")
    .drop("rate_code").drop("store_and_fwd_flag").drop("surcharge")
    .createOrReplaceTempView("ViewDf2012")

  //unifica todos arquivos parquet
  val dfSQLFull = spark.sql("SELECT * FROM ViewDf2009 UNION ALL " +
    "SELECT * FROM ViewDf2010 UNION ALL " +
    "SELECT * FROM ViewDf2011 UNION ALL " +
    "SELECT * FROM ViewDf2012 ORDER BY vendor_id")

}
