package com.claro.Cdrs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import com.claro.Cdrs.processors.KafkaMessageCdr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.{Random, Try}

abstract class MultiFileProcessor(
                                   override val ss: SparkSession,
                                   val environment: String,
                                   val workRepo: String,
                                   val variableName: String,
                                   val jobName: String,
                                   val charset: String,
                                   val path: String,
                                   val separator: String,
                                   val dates: String,
                                   val numFiles: Int,
                                   val kafkaServers: String,
                                   val kafkaTopic: String
                                 ) extends FileProcessor(ss, path) {

  def process(): Unit = {
    val folderDates = getFolderDates(dates)

    val preProcessFolders = getHDFSPreProcessedFolders(path)

    val preProcessList = getFileList(preProcessFolders)

    val validatedList = getValidatedFiles(preProcessList)

    processPreProcessedFiles(validatedList)

    val fileList = getFileList(getFolders(folderDates)).filterNot(file => folderDates.contains(file._1))

    val filesToValidateName = getValidatedNameFiles(fileList)

    val fileNamesError = filesToValidateName.filterNot(_._3).map(file => (file._1, file._2))

    processFileNamesError(fileNamesError)

    val files = filesToValidateName.filter(_._3).map(file => (file._1, file._2))

    val fValidateDuplicated = if (numFiles > 0 && numFiles < files.length) files.slice(0, numFiles) else files

    processValidatedFiles(getValidatedFiles(fValidateDuplicated))

    val dataToValidateError = getDataToProcess

    val errorFiles = processErrorFiles(dataToValidateError)

    val data = if (errorFiles != null && !errorFiles.isEmpty) {
      dataToValidateError.unpersist
      getDataToProcess
    } else
      dataToValidateError

    processNewFiles(data)

    val filesToMove = processLoadControl(data)

    new KafkaMessageCdr(kafkaServers, kafkaTopic)
      .sendMessageToKafkaCdr(data, getColumnToKafka)

    filesToMove.foreach(moveFileToProcessedFolder)
  }

  def isDate(date: String): Boolean = {
    Try {
      val sdf = new SimpleDateFormat("yyyyMMdd")
      sdf.setLenient(false)
      sdf.parse(date)
    }.isSuccess
  }

  def getFolderDates(folders: String): Seq[String] = {
    if (folders != null && !folders.trim.isEmpty)
      folders.split(",")
    else {
      val df = new SimpleDateFormat("yyyyMMdd")
      val calendar = Calendar.getInstance()

      val today = df.format(calendar.getTime)

      calendar.add(Calendar.DATE, -1)

      val yesterday = df.format(calendar.getTime)

      Seq(yesterday, today)
    }
  }

  def getFolders(folders: Seq[String]): Seq[String] = {
    val baseFolder = if (path.endsWith("/")) path else path.concat("/")
    folders.map(baseFolder.concat).filter(isPathExist)
  }

  def getFileList(folders: Seq[String]): Seq[(String, String)] = {
    folders.flatMap(folder => listHDFSFolders(folder)).map(file => (file.split("/").last, file))
  }

  def getFileDate(fileName: String): String = {
    try {
      fileName.split("_")(1)
    } catch {
      case _: Throwable => ""
    }
  }

  def getValidatedNameFiles(files: Seq[(String, String)]): Seq[(String, String, Boolean)] = {
    files.map(file => (file._1, file._2, isDate(getFileDate(file._1))))
  }

  def getValidatedFiles(files: Seq[(String, String)]): Seq[(String, String, Boolean)] = {
    val fileDateList = files.map(file => getFileDate(file._1)).distinct.filter(isDate)
    val dateList = fileDateList.mkString(", ")

    val fileNames = ss.sparkContext.parallelize(files)

    if (dateList.isEmpty) {
      fileNames.map(file => (file._1, file._2, true)).collect
    } else {
      val loadControlSQL = s"SELECT nombre_archivo, fecha_trafico FROM $getLoadControlTable " +
        s"WHERE fecha_trafico IN ($dateList)"

      val loadControl = ss.sql(loadControlSQL).rdd.map(value => (value.getString(0), value.getString(1)))

      fileNames.leftOuterJoin(loadControl)
        .map(file => (file._1, file._2._1, file._2._2.isDefined))
        .collect
    }
  }

  def getDataToProcess: DataFrame = {

    val currentDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

    val folders = getHDFSPreProcessedFolders(path)

    val fileIDs = mutable.Map[String, Long]()

    getFileList(folders).foreach(fileIDs += _._1 -> Math.abs(UUID.randomUUID.getLeastSignificantBits))

    val fileIDFunction: String => Long = file => fileIDs.getOrElse(file, -1)
    val fileIDUDF = udf(fileIDFunction)

    val fileNameFunction: String => String = file => file.split("/").last
    val fileNameUDF = udf(fileNameFunction)

    ss.read
      .option("header", "false")
      .option("delimiter", separator)
      .option("charset", charset)
      .schema(getSchema)
      .csv(folders: _*)
      .withColumn("filepath", input_file_name())
      .withColumn("filename", fileNameUDF(col("filepath")))
      .withColumn("id_archivo_dwh", fileIDUDF(col("filename")))
      .withColumn("fe_carga_dwh", unix_timestamp(lit(currentDate), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
      .withColumn("trafficdate", substring(col(getTimestampField), 0, 8))
  }

  def processFileNamesError(files: Seq[(String, String)]): Unit = {
    files.foreach(file => {
      moveFileToErrorFolder(file._2)
      sendNotification("Error, Archivo con nombre errado: " + file._1)
    })
  }

  def processPreProcessedFiles(files: Seq[(String, String, Boolean)]): Unit = {
    files.foreach(file => if (file._3) moveFileToProcessedFolder(file._2))
  }

  def processValidatedFiles(files: Seq[(String, String, Boolean)]): Unit = {
    files.foreach(file => {
      if (file._3) {
        moveFileToDuplicatedFolder(file._2)
        sendNotification("Warning, archivo duplicado, " + file._1)
      } else
        moveFileToPreProcessedFolder(file._2)
    })
  }

  def processErrorFiles(data: DataFrame): Array[String] = {
    import ss.implicits._

    val badRows = data.filter($"_corrupt_record".isNotNull)
      .select($"filepath")
      .dropDuplicates

    val badFiles = badRows.collect

    badRows.unpersist

    badFiles.foreach(file => {
      val filePath = file.getString(0)
      val fileName = filePath.split("/").last

      moveFileToErrorFolder(filePath)

      sendNotification("Error en el archivo " + fileName + ", contiene errores en la estructura")
    })

    badFiles.map(_.getString(0))
  }

  def processNewFiles(data: DataFrame): Unit = {
    val time = System.currentTimeMillis()
    val random = new Random().nextInt(10000)

    val tempViewName = "CDR_TRAFFIC_TEMP_VIEW_" + time + "_" + random

    data.createOrReplaceTempView(tempViewName)

    ss.sql(getInsertTrafficSQL(tempViewName))
  }

  def processLoadControl(data: DataFrame): Array[String] = {
    import ss.implicits._

    val controlRows = data.groupBy(
      $"filepath", $"id_archivo_dwh", $"filename", $"fe_carga_dwh", $"trafficdate"
    ).agg(
      count("filepath").alias("lines")
    ).cache

    val time = System.currentTimeMillis()
    val random = new Random().nextInt(10000)

    val tempViewName = "CRD_CONTROL_TEMP_VIEW_" + time + "_" + random

    controlRows.createOrReplaceTempView(tempViewName)

    ss.sql(getInsertLoadControlSQL(tempViewName))

    val loadControl = controlRows.map(_.getString(0)).collect

    controlRows.unpersist

    loadControl
  }

  def sendNotification(message: String): Unit = {
    Try {
      val argsNotification = Array(environment, workRepo, variableName, jobName, message)
      //Run_Scen_PK.Run_ssh.main(argsNotification)
      //println("Log080: " + message )
    }
  }

  def getInsertLoadControlSQL(tempViewName: String): String =
    s"INSERT INTO $getLoadControlTable PARTITION (fecha_trafico) " +
      s"SELECT id_archivo_dwh AS consecutivo, filename AS nombre_archivo," +
      s"from_unixtime(unix_timestamp(fe_carga_dwh, 'yyyy-MM-dd HH:mm:ss')) AS fecha_creacion, " +
      s"lines AS cantidad_registros, trafficdate AS fecha_trafico " +
      s"FROM $tempViewName"

  def getLoadControlTable: String

  def getTrafficTable: String

  def getInsertTrafficSQL(tempViewName: String): String

  def getSchema: StructType

  def getTimestampField: String

  def getColumnToKafka: Column
}
