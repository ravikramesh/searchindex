package com.examples.document.searchindex

import org.apache.spark.sql.SparkSession

/**
  * Created at Jun 10, 2019 10:44 PM
  *
  * @author Ravikumar Ramasamy
  *
  */
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
