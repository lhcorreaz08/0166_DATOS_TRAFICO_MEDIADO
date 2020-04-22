package com.claro.Cdrs.processors

import com.claro.Cdrs.core.FirstLastProcessor
import org.apache.spark.sql.functions.{coalesce, concat, lit}
import org.apache.spark.sql.{Column, SparkSession}

class DataVoiceProcessor(
                     override val ss: SparkSession,
                     override val kafkaServers: String,
                     override val kafkaTopic: String
                   )
  extends FirstLastProcessor(
    ss,  kafkaServers, kafkaTopic
  ) {


  override def getColumnToKafka: Column = {
    import ss.implicits._

    concat(
      coalesce($"rowkey", lit("")), lit(","),
      coalesce($"FEC_PRIMER_ACTIV_DATOS", lit("")), lit(","),
      coalesce($"FEC_ULTIMO_ACTIV_DATOS", lit("")), lit(","),
      coalesce($"CELL_ID_PRIMER_EVENTO_DATOS", lit("")), lit(","),
      coalesce($"CELL_ID_ULTIMO_EVENTO_DATOS", lit("")), lit(","),
      coalesce($"ID_LAC_PRIMER_EVENTO_DATOS", lit("")), lit(","),
      coalesce($"ID_LAC_ULTIMO_EVENTO_DATOS", lit("")), lit(","),
      coalesce($"FEC_PRIMER_ACTIV_VOZ_ENT", lit("")), lit(","),
      coalesce($"FEC_ULTIMO_ACTIV_VOZ_ENT", lit("")), lit(","),
      coalesce($"CELL_ID_PRIMER_EVENTO_VOZ_ENT", lit("")), lit(","),
      coalesce($"CELL_ID_ULTIMO_EVENTO_VOZ_ENT", lit("")), lit(","),
      coalesce($"ID_LAC_PRIMER_EVENTO_VOZ_ENT", lit("")), lit(","),
      coalesce($"ID_LAC_ULTIMO_EVENTO_VOZ_ENT", lit("")), lit(","),
      coalesce($"FEC_PRIMER_ACTIV_VOZ_SAL", lit("")), lit(","),
      coalesce($"FEC_ULTIMO_ACTIV_VOZ_SAL", lit("")), lit(","),
      coalesce($"CELL_ID_PRIMER_EVENTO_VOZ_SAL", lit("")), lit(","),
      coalesce($"CELL_ID_ULTIMO_EVENTO_VOZ_SAL", lit("")), lit(","),
      coalesce($"ID_LAC_PRIMER_EVENTO_VOZ_SAL", lit("")), lit(","),
      coalesce($"ID_LAC_ULTIMO_EVENTO_VOZ_SAL", lit(""))
    )
  }

}