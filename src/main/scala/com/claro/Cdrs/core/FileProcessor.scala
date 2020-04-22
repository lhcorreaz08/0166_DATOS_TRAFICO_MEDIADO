package com.claro.Cdrs.core

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._

import scala.collection.mutable
import scala.sys.process._
import scala.util.Try

/**
  * @author David Reniz
  */
class FileProcessor(protected val ss: SparkSession, protected val basePath: String) extends Serializable {

  def listHDFSFolders(path: String): Array[String] = {
    val r = Seq("hdfs", "dfs", "-ls", path).!!
    val s = r.split("\n")
    val t = s.filter(line =>
      !line.equals(s.head)
        && !line.contains("/" + FileProcessor.PROCESSED_FOLDER_NAME)
        && !line.contains("/" + FileProcessor.ERROR_FOLDER_NAME)
        && !line.contains("/" + FileProcessor.DUPLICATED_FOLDER_NAME)
    ).map(line => line.split(" +").last)
    if (t.length == 0) path.split("\n") else t
  }

  def listHDFSPreProcessedFolders(path: String): Array[String] = {
    val prePath = path + (if (path.endsWith("/")) "" else "/") + FileProcessor.PRE_PROCESSED_FOLDER_NAME
    val r = Seq("hdfs", "dfs", "-ls", prePath).!!
    val s = r.split("\n")
    s.filter(line => !line.equals(s.head)).map(line => line.split(" +").last)
  }

  def getHDFSPreProcessedFolders(path: String): Seq[String] = {
    val preProcessedFolders = mutable.ListBuffer[String]()
    val folders = listHDFSPreProcessedFolders(path)
    folders.foreach(folder => {
      if (isPathNotEmpty(folder))
        preProcessedFolders += folder
      else
        FileSystem.get(new URI(folder), ss.sparkContext.hadoopConfiguration)
          .delete(new Path(folder), true)
    })

    preProcessedFolders
  }

  def isPathExist(path: String): Boolean = {
    try {
      FileSystem.get(new URI(path), new Configuration()).exists(new Path(path))
    } catch {
      case _: Throwable => false
    }
  }

  def isPathNotEmpty(path: String): Boolean = {
    try {
      FileSystem.get(new URI(path), new Configuration()).listStatus(new Path(path)).length > 0
    } catch {
      case _: Throwable => false
    }
  }

  private def createFolder(path: String): Unit = {
    if (!isPathExist(path))
      FileSystem.get(new URI(path), ss.sparkContext.hadoopConfiguration).mkdirs(new Path(path))
  }

  protected def moveFileToDuplicatedFolder(file: String): Unit = {
    moveFile(file, FileProcessor.DUPLICATED_FOLDER_NAME)
  }

  protected def moveFileToErrorFolder(file: String): Unit = {
    moveFile(file, FileProcessor.ERROR_FOLDER_NAME)
  }

  protected def moveFileToPreProcessedFolder(file: String): Unit = {
    moveFile(file, FileProcessor.PRE_PROCESSED_FOLDER_NAME)
  }

  protected def moveFileToProcessedFolder(file: String): Unit = {
    moveFile(file, FileProcessor.PROCESSED_FOLDER_NAME)
  }

  private def moveFile(file: String, folder: String): Unit = {
    Try {
      val filePath = file.split("/")
      val dateFolder = filePath(filePath.length - 2)

      val newBasePath = if (basePath.endsWith("/")) basePath else basePath + "/"

      val processedPath = newBasePath + folder + "/" + dateFolder + "/"

      createFolder(processedPath)

      val destinationFile = new Path(processedPath + filePath.last)

      val fs = FileSystem.get(new URI(basePath), ss.sparkContext.hadoopConfiguration)

      if (fs.exists(destinationFile))
        fs.delete(destinationFile, false)

      fs.rename(new Path(file), destinationFile)
    }
  }

}

object FileProcessor {
  private val PRE_PROCESSED_FOLDER_NAME = "PREPROCESADOS"
  private val PROCESSED_FOLDER_NAME = "PROCESADOS"
  private val ERROR_FOLDER_NAME = "ERROR"
  private val DUPLICATED_FOLDER_NAME = "DUPLICADOS"
}
