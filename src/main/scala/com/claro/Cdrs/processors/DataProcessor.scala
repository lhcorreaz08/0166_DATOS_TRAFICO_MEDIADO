package com.claro.Cdrs.processors
import java.text.SimpleDateFormat
import com.claro.Cdrs.core.MultiFileProcessor
import com.claro.Cdrs.entities.Entities
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{coalesce, concat, lit}
import org.apache.spark.sql.types.StructType

class DataProcessor(
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


  override def getLoadControlTable: String = "datos.tbl_datos_control_cargue"



  override def getTrafficTable: String = "datos.tbl_tmp_fact_datos_trafico_tmp"



  override def getSchema: StructType = Entities.dataSchema



  override def getTimestampField: String = "record_opening_time"


    override def getColumnToKafka: Column = {
    import ss.implicits._

    concat(
      coalesce($"served_msisdn", lit("")), lit(","),
      coalesce($"served_imsi", lit("")), lit(","),
      coalesce($"served_imei", lit("")), lit(","),
      coalesce($"record_opening_time", lit("")), lit(","),
      coalesce($"cell_identity", lit("")), lit(","),
      coalesce($"location_area_code", lit(""))
    )
  }

  override def getInsertTrafficSQL(tempViewName: String): String =
    s"INSERT INTO $getTrafficTable PARTITION (fecha_trafico, fec_hora_trafico) " +
      s"SELECT record_type, served_msisdn, served_imsi, served_imei, " +
      s"served_pdp_address, SUBSTRING(served_msisdn, -10, 10), " +
      s"from_unixtime(unix_timestamp(record_opening_time, 'yyyyMMddHHmmss')), " +
      s"from_unixtime(unix_timestamp(record_closing_time, 'yyyyMMddHHmmss')), " +
      s"sgsn_address, ggsn_address, plmnidentifier, location_area_code, " +
      s"routing_area, cell_identity, service_area_code, apnnetwork, system_type, " +
      s"charging_characteristics, user_profile, charginclass, serviceid, " +
      s"uplink, downlink, duration, containerduration, potencial_duplicate, " +
      s"record_sn, chargingid, first_sequence_number, last_sequence_number, " +
      s"cause_for_record_closing, icid, id_archivo, nodo_med, fuenteid, " +
      s"REGEXP_REPLACE(CONTENTURL,'[^a-zA-Z0-9\\u002F-\\u003A-\\u002E-\\u005F-\\u002D]+', ''), " +
      s"fe_carga_dwh, id_archivo_dwh, val_uplink, val_downlink, trafficdate, lpad(hour(current_timestamp()), 2, '0')  " +
      s" FROM $tempViewName"
}