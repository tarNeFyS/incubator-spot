/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spot.proxy

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spot.SuspiciousConnectsArgumentParser.SuspiciousConnectsConfig
import org.apache.spot.{SuspiciousConnectsContext, SuspiciousConnectsScoreFunction}
import org.apache.spot.lda.SpotLDAWrapper
import org.apache.spot.lda.SpotLDAWrapper.{SpotLDAInput, SpotLDAOutput}
import org.apache.spot.proxy.ProxySchema._
import org.apache.spot.utilities._
import org.apache.spot.utilities.data.validation.InvalidDataHandler

/**
  * Encapsulation of a proxy suspicious connections model.
  *
  * @param topicCount         Number of "topics" used to cluster IPs and proxy "words" in the topic modelling analysis.
  * @param ipToTopicMIx       Maps each IP to a vector measuring Prob[ topic | this IP] for each topic.
  * @param wordToPerTopicProb Maps each word to a vector measuring Prob[word | topic] for each topic.
  */
class ProxySuspiciousConnectsModel(topicCount: Int,
                                   ipToTopicMIx: Map[String, Array[Double]],
                                   wordToPerTopicProb: Map[String, Array[Double]]) {

  /**
    * Calculate suspicious connection scores for an incoming dataframe using this proxy suspicious connects model.
    *
    * @param dataFrame Dataframe with columns Host, Time, ReqMethod, FullURI, ResponseContentType, UserAgent, RespCode
    *                  (as defined in ProxySchema object).
    * @return Dataframe with Score column added.
    */
  def score(dataFrame: DataFrame): DataFrame = {
    val context = SuspiciousConnectsContext

    val topDomains: Broadcast[Set[String]] = context.sparkContext.broadcast(TopDomains.TopDomains)

    val agentToCount: Map[String, Long] =
      dataFrame.select(UserAgent).rdd.map({ case Row(ua: String) => (ua, 1L) }).reduceByKey(_ + _).collect().toMap

    val agentToCountBC = context.sparkContext.broadcast(agentToCount)

    val udfWordCreation =
      ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC)

    val wordedDataFrame = dataFrame.withColumn(Word,
      udfWordCreation(dataFrame(Host),
        dataFrame(Time),
        dataFrame(ReqMethod),
        dataFrame(FullURI),
        dataFrame(ResponseContentType),
        dataFrame(UserAgent),
        dataFrame(RespCode)))

    val ipToTopicMixBC = context.sparkContext.broadcast(ipToTopicMIx)
    val wordToPerTopicProbBC = context.sparkContext.broadcast(wordToPerTopicProb)


    val scoreFunction = new SuspiciousConnectsScoreFunction(topicCount, ipToTopicMixBC, wordToPerTopicProbBC)


    def udfScoreFunction = udf((ip: String, word: String) => scoreFunction.score(ip, word))
    wordedDataFrame.withColumn(Score, udfScoreFunction(wordedDataFrame(ClientIP), wordedDataFrame(Word)))
  }
}

/**
  * Contains model creation and training routines.
  */
object ProxySuspiciousConnectsModel {

  // These buckets are optimized to datasets used for training. Last bucket is of infinite size to ensure fit.
  // The maximum value of entropy is given by log k where k is the number of distinct categories.
  // Given that the alphabet and number of characters is finite the maximum value for entropy is upper bounded.
  // Bucket number and size can be changed to provide less/more granularity
  val EntropyCuts = Array(0.0, 0.3, 0.6, 0.9, 1.2,
    1.5, 1.8, 2.1, 2.4, 2.7,
    3.0, 3.3, 3.6, 3.9, 4.2,
    4.5, 4.8, 5.1, 5.4, Double.PositiveInfinity)

  /**
    * Factory for ProxySuspiciousConnectsModel.
    * Trains the model from the incoming DataFrame using the specified number of topics
    * for clustering in the topic model.
    *
    * @param logger       Logge object.
    * @param config       SuspiciousConnetsArgumnetParser.Config object containg CLI arguments.
    * @param inputRecords Dataframe for training data, with columns Host, Time, ReqMethod, FullURI, ResponseContentType,
    *                     UserAgent, RespCode (as defined in ProxySchema object).
    * @return ProxySuspiciousConnectsModel
    */
  def trainNewModel(logger: Logger,
                    config: SuspiciousConnectsConfig,
                    inputRecords: DataFrame): ProxySuspiciousConnectsModel = {

    logger.info("training new proxy suspcious connects model")

    val context = SuspiciousConnectsContext

    val selectedRecords =
      inputRecords.select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, RespCode, FullURI)
      .unionAll(ProxyFeedback.loadFeedbackDF(config.feedbackFile, config.duplicationFactor))



    val agentToCount: Map[String, Long] =
      selectedRecords.select(UserAgent)
        .rdd
        .map({ case Row(agent: String) => (agent, 1L) })
        .reduceByKey(_ + _).collect()
        .toMap

    val agentToCountBC = context.sparkContext.broadcast(agentToCount)



    val docWordCount: RDD[SpotLDAInput] =
      getIPWordCounts(logger, selectedRecords, config.feedbackFile, config.duplicationFactor,
        agentToCount)


    val SpotLDAOutput(ipToTopicMixDF, wordResults) = SpotLDAWrapper.runLDA(docWordCount,
      config.topicCount,
      logger,
      config.ldaPRGSeed,
      config.ldaAlpha,
      config.ldaBeta,
      config.ldaMaxiterations)


    // Since Proxy is still broadcasting ip to topic mix, we need to convert data frame to Map[String, Array[Double]]
    val ipToTopicMix = ipToTopicMixDF
      .rdd
      .map({ case (ipToTopicMixRow: Row) => ipToTopicMixRow.toSeq.toArray })
      .map({
        case (ipToTopicMixSeq) => (ipToTopicMixSeq(0).asInstanceOf[String], ipToTopicMixSeq(1).asInstanceOf[Seq[Double]]
          .toArray)
      })
      .collectAsMap
      .toMap


    new ProxySuspiciousConnectsModel(config.topicCount, ipToTopicMix, wordResults)

  }

  /**
    * Transform proxy log events into summarized words and aggregate into IP-word counts.
    * Returned as [[SpotLDAInput]] objects.
    *
    * @return RDD of [[SpotLDAInput]] objects containing the aggregated IP-word counts.
    */
  def getIPWordCounts(logger: Logger,
                      inputRecords: DataFrame,
                      feedbackFile: String,
                      duplicationFactor: Int,
                      agentToCount: Map[String, Long]): RDD[SpotLDAInput] = {


    logger.info("Read source data")
    val selectedRecords = inputRecords.select(Date, Time, ClientIP, Host, ReqMethod, UserAgent, ResponseContentType, RespCode, FullURI)

    val wc = ipWordCountFromDF(selectedRecords, agentToCount)
    logger.info("proxy pre LDA completed")

    wc
  }

  def ipWordCountFromDF(dataFrame: DataFrame,
                        agentToCount: Map[String, Long]): RDD[SpotLDAInput] = {
    val context = SuspiciousConnectsContext

    val topDomains: Broadcast[Set[String]] = context.sparkContext.broadcast(TopDomains.TopDomains)

    val agentToCountBC = context.sparkContext.broadcast(agentToCount)
    val udfWordCreation = ProxyWordCreation.udfWordCreation(topDomains, agentToCountBC)

    val ipWord = dataFrame.withColumn(Word,
      udfWordCreation(dataFrame(Host),
        dataFrame(Time),
        dataFrame(ReqMethod),
        dataFrame(FullURI),
        dataFrame(ResponseContentType),
        dataFrame(UserAgent),
        dataFrame(RespCode)))
      .select(ClientIP, Word)

    ipWord
      .filter(ipWord(Word).notEqual(InvalidDataHandler.WordError))
      .rdd
      .map({ case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1) })
      .reduceByKey(_ + _).map({ case ((ip, word), count) => SpotLDAInput(ip, word, count) })
  }
}