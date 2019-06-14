package com.examples.document.searchindex

import org.apache.spark.sql.Dataset
import org.scalatest.FunSpec

/**
  * Created at Jun 13, 2019 3:26 PM
  *
  * @author Ravikumar Ramasamy
  *
  */
class DocumentReaderSpec extends FunSpec with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Stop word counts") {

    it("returns a count of stop words to 2") {

      val results = DocumentReader.stopWords()

      assert(results.size == 2)

    }

  }

  describe("Read documents") {

    it("should read the documents") {

      val path = "/tmp/testdocument"

      val documentDF = Seq("Ravikumar was in San Francisco to attend Spark Summit ", "Hello testing").toDF().
        coalesce(1).write.text(path)

      val records : Dataset[Record] = DocumentReader.readDocuments(path)

      assert(records.count === 2)

    }
  }

}
