package com.examples.document.searchindex

import com.typesafe.config.ConfigFactory

/**
  * Created at Jun 09, 2019 8:38 AM
  *
  * @author Ravikumar Ramasamy
  *
  */
object JobConfig {

  val appConfiguration = ConfigFactory.load()

  val documentDictionaryFileName =  appConfiguration.getString("filename.documentdictionary")

  val wordDictionaryFileName = appConfiguration.getString("filename.worddictionary")

  val invertedIndexFileName = appConfiguration.getString("filename.invertedindex")

  val cacheStorageLevel = appConfiguration.getString("cache.storagelevel")

}


