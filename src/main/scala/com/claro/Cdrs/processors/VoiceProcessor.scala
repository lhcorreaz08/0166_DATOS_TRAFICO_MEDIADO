package com.claro.Cdrs.processors

import com.claro.Cdrs.core.MultiFileProcessor
import com.claro.Cdrs.entities.Entities
import org.apache.spark.sql.functions.{coalesce, concat, lit}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.StructType

class VoiceProcessor(
                      override val ss: SparkSession,
                      override val environment: String,
                      override val workRepo: String,
                      override val variableName: String,
                      override val jobName: String,
                      override val charset: String,
                      override val path: String,
                      override val separator: String,
                      override val dates: String,
                      override val numFiles: Int,
                      override val kafkaServers: String,
                      override val kafkaTopic: String
                    )
  extends MultiFileProcessor(
    ss, environment, workRepo, variableName, jobName, charset, path, separator,
    dates, numFiles, kafkaServers, kafkaTopic
  ) {

  override def getLoadControlTable: String = "voz.tbl_voz_control_cargue"

  override def getTrafficTable: String = "voz.tbl_fact_voz_trafico"

  override def getSchema: StructType = Entities.voiceSchema

  override def getTimestampField: String = "start_time"

  override def getColumnToKafka: Column = {
    import ss.implicits._

    concat(
      coalesce($"calling_number", lit("")), lit(","),
      coalesce($"called_number", lit("")), lit(","),
      coalesce($"call_type", lit("")), lit(","),
      coalesce($"calling_serial_number", lit("")), lit(","),
      coalesce($"imsi", lit("")), lit(","),
      coalesce($"start_time", lit("")), lit(","),
      coalesce($"cell_in", lit("")), lit(","),
      coalesce($"subs_first_lac", lit(""))
    )
  }

  override def getInsertTrafficSQL(tempViewName: String): String =
    s"INSERT INTO $getTrafficTable PARTITION (fecha_trafico) " +
      s"SELECT camel_call_ref, camel_exchange_id_ton, camel_exchange_id, " +
      s"calling_number, dialled_number, called_number, calling_serial_number, " +
      s"call_type, from_unixtime(unix_timestamp(in_channel, 'yyyyMMddHHmmss')), " +
      s"call_duration, from_unixtime(unix_timestamp(start_time, 'yyyyMMddHHmmss')), " +
      s"call_ref, routing_category, routing_category_add, completion_code, imsi, " +
      s"first_originating_cli, first_terminating_cli, switch, cug_information, " +
      s"sistema_destino, celda_hexadecimal, file_name, tecnologia, " +
      s"id_archivo_dwh, fe_carga_dwh, " +
      s"first_originating_cli AS trunk_in, first_terminating_cli AS trunk_out, cell_in, " +
      s"cell_out, call_duration_in_channel, subsistema_destino, query_status, nrno, " +
      s"nrnd, tipob, facility_usage, subs_first_lac, subs_last_lac, redirecting_number, " +
      s"msclassmark, codec, status_suscriber, charging_type, ggsn_address, charging_id, " +
      s"open_tas_id, icid_value, trafficdate FROM $tempViewName"
}