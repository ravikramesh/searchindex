package com.examples.document.searchindex

import org.apache.spark.sql.SparkSession

/**
  * Created at Jun 13, 2019 12:31 PM
  *
  * @author Ravikumar Ramasamy
  *
  */
object SparkSessionConfig extends Serializable  {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("InvertIndex Job")
    .master("yarn")
    .getOrCreate()


}
