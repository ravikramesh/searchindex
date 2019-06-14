package com.examples.document.searchindex

import com.examples.document.searchindex.DocumentReader._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created at Jun 09, 2019 8:35 AM
  *
  * @author Ravikumar Ramasamy
  *
  */
object IndexGeneratorApp  extends App {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val args1 = Array[String]("file:///Users/ramasar1/Downloads/test/data","file:///Users/ramasar1/Downloads/test/results_out1162")

  require(args.length >= 2,
    """Please Provide all the required job configuration parameters.
      | Usage : [searchPath] [output_path]
    """.stripMargin)

  implicit val (searchPath, outputPath) = (args(0), args(1))

  lazy val spark : SparkSession = SparkSessionConfig.spark

  val stopWordsBC = spark.sparkContext.broadcast(stopWords())

  val documentIdMap: Map[String, Int] = getFileList(searchPath).zipWithIndex.toMap

  val documentIdMapBC = spark.sparkContext.broadcast(documentIdMap)

  try {

    saveDocumentDictionary(outputPath, convertDocDictionaryToDF)

    import SparkSessionConfig.spark.implicits._

    val documentDF = readDocuments(searchPath)
      .mapPartitions(iter => processSinglePartitionData(iter)).as[WordDocumentId]
      .groupBy(col("word"))
      .agg(collect_set("docId").as("doc_list"))
      .dropDuplicates()
      .withColumn("word_id", monotonically_increasing_id().alias("word_id"))
      .orderBy(col("word_id"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    buildAndSaveInvertedIndex(documentDF, outputPath)

    saveWordDictionary(documentDF, outputPath)

    documentDF.unpersist()

  } finally {

    stopWordsBC.unpersist()

    documentIdMapBC.unpersist()
  }

  logger.info("Inverted index has been generated successfully.")


  def convertDocDictionaryToDF() = {
    import spark.implicits._
    documentIdMap.toList
      .map( rec => (rec._1.substring(rec._1.indexOf(searchPath)+searchPath.length), rec._2))
      .toDF("document","id")
      .select(concat(lit("'"), col("document"), lit("' : "), col("id"), lit(",")))
  }


  def processSinglePartitionData( records: Iterator[Record]) : Iterator[WordDocumentId] = {
    val uniqueWords = mutable.HashSet[WordDocumentId]()
    var fileName: Option[String]  = None
    records.foreach(rec => {
      if(fileName == None) {
        fileName = Some(rec.filepath)
      }

      rec.word
        .replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
        .split("[,;\\s]+")
        .distinct
        .filter( word => !stopWordsBC.value.contains(word))
        .map( word => WordDocumentId(word,documentIdMapBC.value.get(fileName.get).getOrElse(-1)))
        .foreach( wordDocId => uniqueWords.add(wordDocId))
    })
    uniqueWords.iterator
  }

  def saveDocumentDictionary (outputPath : String, docDF : DataFrame) = {
    saveAsTextFile(docDF, s"${outputPath}/${JobConfig.documentDictionaryFileName}")
  }

  def buildAndSaveInvertedIndex (documentDF: DataFrame, outputPath: String) = {
    saveAsTextFile(transformInvertIndex(documentDF), s"${outputPath}/${JobConfig.invertedIndexFileName}")
    logger.info(s" Inverted Index dictionary saved in ${outputPath}/${JobConfig.invertedIndexFileName} ")
  }

  def saveWordDictionary  (documentDF: DataFrame, outputPath: String) = {
    saveAsTextFile(transformWordDictionary(documentDF), s"${outputPath}/${JobConfig.wordDictionaryFileName}")
    logger.info(s" Word Ditionary saved in ${outputPath}/${JobConfig.wordDictionaryFileName} ")
  }

  def transformInvertIndex  (documentDF: DataFrame) = {
    documentDF
      .select(concat(col("word_id"), lit(" : ["),  concat_ws(",",col("doc_list")), lit("],")))
  }

  def transformWordDictionary (documentDF: DataFrame) = {
    documentDF
      .select(concat(lit("'"), col("word"), lit("' : "), col("word_id"), lit(",")))
  }

  def saveAsTextFile ( documentDF: DataFrame, filepath: String) = {
    documentDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .text(filepath)
  }



}
