package org.apache.spot

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Context containing the Spark related context
  */
object SuspiciousConnectsContext {

  val analysis = "TOTO" //TODO : replace with a the proper value from config

  val sparkConfig:SparkConf = new SparkConf().setAppName("Spot ML:  " + analysis + " suspicious connects analysis")
  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new SQLContext(sparkContext)

  def SuspiciousConnectsContext()
}
