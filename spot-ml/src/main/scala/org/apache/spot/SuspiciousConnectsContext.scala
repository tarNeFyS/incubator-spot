package org.apache.spot

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by david on 07/06/17.
  */
object SuspiciousConnectsContext {

  val analysis = "TOTO" //TODO : replace with a the proper value from config

  val sparkConfig:SparkConf = new SparkConf().setAppName("Spot ML:  " + analysis + " suspicious connects analysis")
  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new SQLContext(sparkContext)

  def getSparkContext: SparkContext = {
    sparkContext
  }

  def getSQLContext: SQLContext = {
    sqlContext
  }



}
