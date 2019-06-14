package com.examples.document.searchindex

import org.scalatest.FunSpec

import scala.reflect.io.File
import scala.util.Random

/**
  * Created at Jun 13, 2019 10:25 PM
  *
  * @author Ravikumar Ramasamy
  *
  */
class IndexGeneratorAppSpec extends FunSpec with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Generate invert index file") {

    it("returns a 17 words in word dictionary after removing duplicates and stopwords") {


      val inputPath = "/tmp/testdocument"+Random.nextInt()

      val documentDF1 = Seq("Ravikumar was in San Francisco to attend Spark Summit ", "This year Spark Summit held on April","Today it hot weather")
        .toDF().coalesce(1)
        .write.text(inputPath)

      val outputPath = "/tmp/result"+Random.nextInt()

      val args = Array(inputPath,outputPath)
      IndexGeneratorApp.main(args)


      val invertedIndexContents = spark.read.textFile(outputPath+"/"+JobConfig.invertedIndexFileName).collect()
      val documentDictionary = spark.read.textFile(outputPath+"/"+JobConfig.documentDictionaryFileName).collect()
      val wordDictionary = spark.read.textFile(outputPath+"/"+JobConfig.wordDictionaryFileName).collect()

      println(wordDictionary.toList)
      println(documentDictionary.toList)
      println(invertedIndexContents.toList)

      assert(documentDictionary.toList.length == 2)

      assert(wordDictionary.toList.length == 17)

      assert(invertedIndexContents.toList.length == 17)


      File(inputPath).deleteRecursively()
      File(outputPath).deleteRecursively()

    }

  }


}
