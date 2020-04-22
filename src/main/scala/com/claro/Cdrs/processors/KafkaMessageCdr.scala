package com.claro.Cdrs.processors

import org.apache.spark.sql.{Column, DataFrame}

class KafkaMessageCdr(val kafkaServers: String, val kafkaTopic: String) {

  def sendMessageToKafkaCdr(data: DataFrame, column: Column): Unit = {

    data.select(column.alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", kafkaTopic)
      .save
  }
}
