package com.examples.document.searchindex

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{input_file_name, _}

import scala.collection.mutable
import scala.io.Source

/**
  * Created at Jun 10, 2019 9:29 PM
  *
  * @author Ravikumar Ramasamy
  *
  */
object DocumentReader extends Serializable {

  def readDocuments(searchPath: String) : Dataset[Record] = {

    import SparkSessionConfig.spark.implicits._

    SparkSessionConfig.spark.sparkContext.textFile(searchPath)
      .toDF("word")
      .select(col("word").as("word"), input_file_name().alias("filepath")).as[Record]
  }

  def getFileList(searchPath: String)  : mutable.HashSet[String] = {
    val inputPaths = searchPath.split(",").map( rec => new Path(rec))
    implicit  val fs = FileSystem.get(SparkSessionConfig.spark.sparkContext.hadoopConfiguration)
    val fileList : mutable.HashSet[String] = mutable.HashSet()
    inputPaths.foreach( fpath => {
       getRecursiveListOfFiles(fpath)
         .map( fstatus => fstatus.getPath.toString)
         .foreach( file => {
           fileList.add(file)
         })
    })
    fileList
  }


  private def getRecursiveListOfFiles(fpath: Path) (implicit fs: FileSystem) : Array[FileStatus] = {
    val these = fs.listStatus(fpath)
    these ++ these.filter(_.isDirectory).flatMap( rec => getRecursiveListOfFiles(rec.getPath))
  }

  private [searchindex] def stopWords() : Set[String] = {
    val source = Source.fromInputStream(getClass().getClassLoader.getResourceAsStream("stopwords.txt"))
    source
      .getLines().toSet
  }
}
