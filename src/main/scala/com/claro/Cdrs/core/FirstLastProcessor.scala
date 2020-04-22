package com.claro.Cdrs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.claro.Cdrs.processors.KafkaMessageCdr
import org.apache.spark.sql.functions.{coalesce, concat, lit}
import org.apache.spark.sql.{Column, SparkSession}

abstract class FirstLastProcessor(
                                   val ss: SparkSession,
                                   val kafkaServers: String,
                                   val kafkaTopic: String
                                 )  {



  def getSelectBD(): (String, String, String) = {

    val fechaConsulta = Calendar.getInstance()
    val fechaTrafico = Calendar.getInstance()
    val currentDay = Calendar.getInstance()
    fechaConsulta.add(Calendar.HOUR_OF_DAY, -2)
    val fechaInicio = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(fechaConsulta.getTime())
    fechaTrafico.add(Calendar.DAY_OF_MONTH, -1)
    val fechaDesde = new SimpleDateFormat("yyyyMMdd").format(fechaTrafico.getTime())
    val fechaHasta = new SimpleDateFormat("yyyyMMdd").format(currentDay.getTime())
    //========================================
    //maximos y minimos de datos
    //========================================

    val selectDatos: String = s"with MAXIMOS as ( " +
      s"SELECT  A.rowkey, A.FEC_ULTIMO_ACTIV_DATOS, A.CELL_ID_ULTIMO_EVENTO_DATOS, A.ID_LAC_ULTIMO_EVENTO_DATOS  FROM(   " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey , date_format(FEC_ULTIMO_ACTIV_DATOS,'yyyyMMddHHmmss') AS FEC_ULTIMO_ACTIV_DATOS, CELL_ID_ULTIMO_EVENTO_DATOS, ID_LAC_ULTIMO_EVENTO_DATOS,  " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_ULTIMO_ACTIV_DATOS DESC ) AS row_num " +
      s"FROM (  " +
      s"SELECT concat(cast(served_msisdn as VARCHAR(20)) ,'_' ,SUBSTRING(served_imei,1,14) ,'_' ,served_imsi) as rowkey, " +
      s"record_opening_time as FEC_ULTIMO_ACTIV_DATOS, cell_identity as CELL_ID_ULTIMO_EVENTO_DATOS, location_area_code as ID_LAC_ULTIMO_EVENTO_DATOS " +
      s"from datos.tbl_fact_datos_trafico where  record_opening_time>= '" + fechaInicio + "' and fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  " +
      s"AND length(cast(served_msisdn as VARCHAR(20)))=10 and served_imei!='0000000000000000' and served_imsi!='0000000000000000'  " +
      s"and length(served_imei)>0 and LENGTH(cast(served_imsi as string))>14 and served_imei is not null and served_imsi is not null " +
      s"UNION ALL  " +
      s"select concat(SUBSTRING(cast(served_msisdn as VARCHAR(20)),3,10) ,'_' , SUBSTRING(served_imei,1,14) ,'_' , served_imsi) as rowkey, " +
      s"record_opening_time as FEC_ULTIMO_ACTIV_DATOS, cell_identity as CELL_ID_ULTIMO_EVENTO_DATOS, location_area_code as ID_LAC_ULTIMO_EVENTO_DATOS " +
      s"from datos.tbl_fact_datos_trafico where  record_opening_time >= '" + fechaInicio + "' and  fecha_trafico in ('" + fechaDesde + "','" + fechaHasta + "')  " +
      s"AND length(cast(served_msisdn as VARCHAR(20)))=12  " +
      s"AND SUBSTRING(cast(served_msisdn as VARCHAR(20)),1,2)='57' and served_imei!='0000000000000000' and served_imsi!='0000000000000000'  " +
      s"and length(served_imei)>0 and LENGTH(cast(served_imsi as string))>14 and served_imei is not null and served_imsi is not null  " +
      s") AS DATOS  " +
      s")AS A " +
      s"WHERE A.row_num = 1), " +
      s"MINIMOS AS ( " +
      s"SELECT  A.rowkey, A.FEC_PRIMER_ACTIV_DATOS, A.CELL_ID_PRIMER_EVENTO_DATOS, A.ID_LAC_PRIMER_EVENTO_DATOS FROM(   " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey , date_format(FEC_PRIMER_ACTIV_DATOS,'yyyyMMddHHmmss') AS FEC_PRIMER_ACTIV_DATOS, CELL_ID_PRIMER_EVENTO_DATOS, ID_LAC_PRIMER_EVENTO_DATOS, " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_PRIMER_ACTIV_DATOS ASC ) AS row_num " +
      s"FROM (  " +
      s"SELECT concat( cast(served_msisdn as VARCHAR(20)),'_' ,SUBSTRING(served_imei,1,14) ,'_' , served_imsi) as rowkey, " +
      s"record_opening_time as FEC_PRIMER_ACTIV_DATOS, cell_identity as CELL_ID_PRIMER_EVENTO_DATOS, location_area_code as ID_LAC_PRIMER_EVENTO_DATOS " +
      s"from datos.tbl_fact_datos_trafico where  record_opening_time>= '" + fechaInicio + "' and fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  " +
      s"AND length(cast(served_msisdn as VARCHAR(20)))=10 and served_imei!='0000000000000000' and served_imsi!='0000000000000000'  " +
      s"and length(served_imei)>0 and LENGTH(cast(served_imsi as string))>14 and served_imei is not null and served_imsi is not null " +
      s"UNION ALL  " +
      s"select  concat(SUBSTRING(cast(served_msisdn as VARCHAR(20)),3,10) , '_' , SUBSTRING(served_imei,1,14) ,'_' , served_imsi) as rowkey,  " +
      s"record_opening_time as FEC_PRIMER_ACTIV_DATOS, cell_identity as CELL_ID_PRIMER_EVENTO_DATOS, location_area_code as ID_LAC_PRIMER_EVENTO_DATOS " +
      s"from datos.tbl_fact_datos_trafico where  record_opening_time >= '" + fechaInicio + "' and fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  " +
      s"AND length(cast(served_msisdn as VARCHAR(20)))=12  " +
      s"AND SUBSTRING(cast(served_msisdn as VARCHAR(20)),1,2)='57' and served_imei!='0000000000000000' and served_imsi!='0000000000000000'  " +
      s"and length(served_imei)>0 and LENGTH(cast(served_imsi as string))>14 and served_imei is not null and served_imsi is not null  " +
      s") AS DATOS  " +
      s")AS A " +
      s"WHERE A.row_num = 1 " +
      s") " +
      s"SELECT M.rowkey, N.FEC_PRIMER_ACTIV_DATOS, M.FEC_ULTIMO_ACTIV_DATOS, N.CELL_ID_PRIMER_EVENTO_DATOS, M.CELL_ID_ULTIMO_EVENTO_DATOS,N.ID_LAC_PRIMER_EVENTO_DATOS, M.ID_LAC_ULTIMO_EVENTO_DATOS " +
      s"FROM MAXIMOS M LEFT JOIN MINIMOS N " +
      s"ON M.rowkey = N.rowkey"


    //========================================
    //Voz entrante
    //========================================
    val selectVozEnt: String = s"with MAXIMOS as ( " +
      s"SELECT  A.rowkey, A.FEC_ULTIMO_ACTIV_VOZ_ENT, A.CELL_ID_ULTIMO_EVENTO_VOZ_ENT, A.ID_LAC_ULTIMO_EVENTO_VOZ_ENT FROM(  " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey, FEC_ULTIMO_ACTIV_VOZ_ENT, CELL_ID_ULTIMO_EVENTO_VOZ_ENT, ID_LAC_ULTIMO_EVENTO_VOZ_ENT, " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_ULTIMO_ACTIV_VOZ_ENT DESC ) AS row_num  " +
      s"FROM (   " +
      s"SELECT concat( called_number ,'_', SUBSTRING(calling_serial_number,1,14),'_',cast(imsi as bigint)) as rowkey,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_ULTIMO_ACTIV_VOZ_ENT ,cell_in as CELL_ID_ULTIMO_EVENTO_VOZ_ENT, " +
      s"subs_first_lac as ID_LAC_ULTIMO_EVENTO_VOZ_ENT " +
      s"from voz.tbl_fact_voz_trafico  " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(2,11,14) AND start_time >= '" + fechaInicio + "'  " +
      s"AND length(called_number)=10 and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'   " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14 and calling_serial_number is not null and imsi is not null  " +
      s"UNION ALL  " +
      s"select CONCAT( SUBSTRING(called_number,3,10) ,'_', SUBSTRING(calling_serial_number,1,14)  ,'_', cast(imsi as bigint)) as rowkey ,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_ULTIMO_ACTIV_VOZ_ENT ,cell_in as CELL_ID_ULTIMO_EVENTO_VOZ_ENT, " +
      s"subs_first_lac as ID_LAC_ULTIMO_EVENTO_VOZ_ENT  " +
      s"from voz.tbl_fact_voz_trafico   " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(2,11,14) AND start_time >=  '" + fechaInicio + "' " +
      s"and length(called_number)=12 and substr(called_number,1,2)='57' and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'  " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14  and calling_serial_number is not null and imsi is not null  " +
      s") AS VOZ  " +
      s")AS A  " +
      s"where A.row_num = 1 " +
      s"), " +
      s"MINIMOS AS ( " +
      s"SELECT  A.rowkey, A.FEC_PRIMER_ACTIV_VOZ_ENT, A.CELL_ID_PRIMER_EVENTO_VOZ_ENT, A.ID_LAC_PRIMER_EVENTO_VOZ_ENT FROM(  " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey, FEC_PRIMER_ACTIV_VOZ_ENT, CELL_ID_PRIMER_EVENTO_VOZ_ENT, ID_LAC_PRIMER_EVENTO_VOZ_ENT, " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_PRIMER_ACTIV_VOZ_ENT ASC ) AS row_num  " +
      s"FROM (   " +
      s"SELECT concat( called_number ,'_', SUBSTRING(calling_serial_number,1,14),'_',cast(imsi as bigint)) as rowkey,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_PRIMER_ACTIV_VOZ_ENT ,cell_in as CELL_ID_PRIMER_EVENTO_VOZ_ENT, " +
      s"subs_first_lac as ID_LAC_PRIMER_EVENTO_VOZ_ENT " +
      s"from voz.tbl_fact_voz_trafico  " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(2,11,14) AND start_time >= '" + fechaInicio + "' " +
      s"AND length(called_number)=10 and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'   " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14 and calling_serial_number is not null and imsi is not null " +
      s"UNION ALL  " +
      s"select CONCAT( SUBSTRING(called_number,3,10) ,'_', SUBSTRING(calling_serial_number,1,14)  ,'_', cast(imsi as bigint)) as rowkey ,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_PRIMER_ACTIV_VOZ_ENT ,cell_in as CELL_ID_PRIMER_EVENTO_VOZ_ENT, " +
      s"subs_first_lac as ID_LAC_PRIMER_EVENTO_VOZ_ENT  " +
      s"from voz.tbl_fact_voz_trafico   " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(2,11,14) AND start_time >= '" + fechaInicio + "' " +
      s"and length(called_number)=12 and substr(called_number,1,2)='57' and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'  " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14  and calling_serial_number is not null and imsi is not null  " +
      s") AS VOZ  " +
      s")AS A  " +
      s"where A.row_num = 1) " +
      s"SELECT M.rowkey, N.FEC_PRIMER_ACTIV_VOZ_ENT, M.FEC_ULTIMO_ACTIV_VOZ_ENT, N.CELL_ID_PRIMER_EVENTO_VOZ_ENT, M.CELL_ID_ULTIMO_EVENTO_VOZ_ENT, " +
      s"N.ID_LAC_PRIMER_EVENTO_VOZ_ENT, M.ID_LAC_ULTIMO_EVENTO_VOZ_ENT FROM MAXIMOS M LEFT JOIN MINIMOS N ON M.rowkey = N.rowkey"



    //========================================
    //Voz saliente
    //========================================

    val selectVozSal: String = s"with MAXIMOS as ( " +
      s"SELECT  A.rowkey, A.FEC_ULTIMO_ACTIV_VOZ_SAL, A.CELL_ID_ULTIMO_EVENTO_VOZ_SAL, A.ID_LAC_ULTIMO_EVENTO_VOZ_SAL FROM(  " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey, FEC_ULTIMO_ACTIV_VOZ_SAL, CELL_ID_ULTIMO_EVENTO_VOZ_SAL, ID_LAC_ULTIMO_EVENTO_VOZ_SAL, " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_ULTIMO_ACTIV_VOZ_SAL DESC ) AS row_num  " +
      s"FROM (   " +
      s"SELECT concat( calling_number ,'_', SUBSTRING(calling_serial_number,1,14),'_',cast(imsi as bigint)) as rowkey,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_ULTIMO_ACTIV_VOZ_SAL ,cell_in as CELL_ID_ULTIMO_EVENTO_VOZ_SAL, " +
      s"subs_first_lac as ID_LAC_ULTIMO_EVENTO_VOZ_SAL " +
      s"from voz.tbl_fact_voz_trafico  " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(1,13) AND start_time >= '" + fechaInicio + "'  " +
      s"AND length(calling_number)=10 and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'   " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14 and calling_serial_number is not null and imsi is not null  " +
      s"UNION ALL  " +
      s"select CONCAT( SUBSTRING(calling_number,3,10) ,'_', SUBSTRING(calling_serial_number,1,14)  ,'_', cast(imsi as bigint)) as rowkey ,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_ULTIMO_ACTIV_VOZ_SAL ,cell_in as CELL_ID_ULTIMO_EVENTO_VOZ_SAL, " +
      s"subs_first_lac as ID_LAC_ULTIMO_EVENTO_VOZ_SAL  " +
      s"from voz.tbl_fact_voz_trafico   " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(1,13) AND start_time >=  '" + fechaInicio + "' " +
      s"and length(calling_number)=12 and substr(calling_number,1,2)='57' and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'  " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14  and calling_serial_number is not null and imsi is not null  " +
      s") AS VOZ  " +
      s")AS A  " +
      s"where A.row_num = 1 " +
      s"), " +
      s"MINIMOS AS ( " +
      s"SELECT  A.rowkey, A.FEC_PRIMER_ACTIV_VOZ_SAL, A.CELL_ID_PRIMER_EVENTO_VOZ_SAL, A.ID_LAC_PRIMER_EVENTO_VOZ_SAL FROM(  " +
      s"SELECT cast (rowkey as varchar(60)) as rowkey, FEC_PRIMER_ACTIV_VOZ_SAL, CELL_ID_PRIMER_EVENTO_VOZ_SAL, ID_LAC_PRIMER_EVENTO_VOZ_SAL, " +
      s"ROW_NUMBER() OVER (PARTITION BY rowkey ORDER BY FEC_PRIMER_ACTIV_VOZ_SAL ASC ) AS row_num  " +
      s"FROM (   " +
      s"SELECT concat( calling_number ,'_', SUBSTRING(calling_serial_number,1,14),'_',cast(imsi as bigint)) as rowkey,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_PRIMER_ACTIV_VOZ_SAL ,cell_in as CELL_ID_PRIMER_EVENTO_VOZ_SAL, " +
      s"subs_first_lac as ID_LAC_PRIMER_EVENTO_VOZ_SAL " +
      s"from voz.tbl_fact_voz_trafico  " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(1,13) AND start_time >= '" + fechaInicio + "' " +
      s"AND length(calling_number)=10 and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'   " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14 and calling_serial_number is not null and imsi is not null " +
      s"UNION ALL  " +
      s"select CONCAT( SUBSTRING(calling_number,3,10) ,'_', SUBSTRING(calling_serial_number,1,14)  ,'_', cast(imsi as bigint)) as rowkey ,  " +
      s"date_format(start_time,'yyyyMMddHHmmss') as FEC_PRIMER_ACTIV_VOZ_SAL ,cell_in as CELL_ID_PRIMER_EVENTO_VOZ_SAL, " +
      s"subs_first_lac as ID_LAC_PRIMER_EVENTO_VOZ_SAL  " +
      s"from voz.tbl_fact_voz_trafico   " +
      s"where fecha_trafico in ('" + fechaDesde + "', '" + fechaHasta + "')  and call_type in(1,13) AND start_time >= '" + fechaInicio + "' " +
      s"and length(calling_number)=12 and substr(calling_number,1,2)='57' and calling_serial_number!='0000000000000000' and imsi!='0000000000000000'  " +
      s"and length(calling_serial_number)>0 and LENGTH(imsi)>14  and calling_serial_number is not null and imsi is not null  " +
      s") AS VOZ  " +
      s")AS A  " +
      s"where A.row_num = 1) " +
      s"SELECT M.rowkey, N.FEC_PRIMER_ACTIV_VOZ_SAL, M.FEC_ULTIMO_ACTIV_VOZ_SAL, N.CELL_ID_PRIMER_EVENTO_VOZ_SAL, M.CELL_ID_ULTIMO_EVENTO_VOZ_SAL, " +
      s"N.ID_LAC_PRIMER_EVENTO_VOZ_SAL, M.ID_LAC_ULTIMO_EVENTO_VOZ_SAL FROM MAXIMOS M LEFT JOIN MINIMOS N ON M.rowkey = N.rowkey"


    (selectDatos, selectVozEnt, selectVozSal)
  }


  def getSelectDF(): (String, String, String) = {

    val formatOut_ = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    val currentd_ = formatOut_.format(new Date(System.currentTimeMillis())).toString


    val dfDatos: String = "select  rowkey, FEC_PRIMER_ACTIV_DATOS, FEC_ULTIMO_ACTIV_DATOS, CELL_ID_PRIMER_EVENTO_DATOS, " +
      "CELL_ID_ULTIMO_EVENTO_DATOS, ID_LAC_PRIMER_EVENTO_DATOS, ID_LAC_ULTIMO_EVENTO_DATOS " +
      "from datos"

    val dfVozEnt: String = "select rowkey, FEC_PRIMER_ACTIV_VOZ_ENT, FEC_ULTIMO_ACTIV_VOZ_ENT, CELL_ID_PRIMER_EVENTO_VOZ_ENT, CELL_ID_ULTIMO_EVENTO_VOZ_ENT, " +
      "ID_LAC_PRIMER_EVENTO_VOZ_ENT, ID_LAC_ULTIMO_EVENTO_VOZ_ENT " +
      "from vozEnt"

    val dfVozSal: String = "select rowkey, FEC_PRIMER_ACTIV_VOZ_SAL, FEC_ULTIMO_ACTIV_VOZ_SAL, CELL_ID_PRIMER_EVENTO_VOZ_SAL, CELL_ID_ULTIMO_EVENTO_VOZ_SAL, " +
      "ID_LAC_PRIMER_EVENTO_VOZ_SAL, ID_LAC_ULTIMO_EVENTO_VOZ_SAL " +
      "from vozSal"


    (dfDatos, dfVozEnt, dfVozSal)

  }



  def process(): Unit = {

    val sQueryBD = getSelectBD()
    val sqDatos: String = sQueryBD._1
    val sqVozEnt: String = sQueryBD._2
    val sqVozSal: String = sQueryBD._3


    val sQueryDF = getSelectDF

    val dfDatos_ = ss.sqlContext.sql(sqDatos)
    dfDatos_.createOrReplaceTempView("datos")
    val dfDatos = ss.sql(sQueryDF._1).na.fill("")
    // log("filas de hive datos:" + dfDatos.count())

    val dfVozEnt = ss.sqlContext.sql(sqVozEnt)
    dfVozEnt.createOrReplaceTempView("vozEnt")
    val dfVozEnt_ = ss.sql(sQueryDF._2).na.fill("")
    //log("filas de hive voz entrante:" + dfVozEnt_.count())


    val dfVozSal = ss.sqlContext.sql(sqVozSal)
    dfVozSal.createOrReplaceTempView("vozSal")
    val dfVozSal_ = ss.sql(sQueryDF._3).na.fill("")
    //log("filas de hive voz Saliente:" + dfVozSal_.count())

    val dfResumen_ = dfDatos_.join(dfVozEnt_, Seq("rowkey"), "outer").
      join(dfVozSal_, Seq("rowkey"), "outer")//.coalesce(100).groupBy("rowkey")


    new KafkaMessageCdr(kafkaServers, kafkaTopic)
      .sendMessageToKafkaCdr(dfResumen_, getColumnToKafka)


    println("Log0566: fin")

  }

   def getColumnToKafka: Column = {
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
