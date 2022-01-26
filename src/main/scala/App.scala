import com.claro.Cdrs.core.MultiFileProcessor
import com.claro.Cdrs.processors.{DataProcessor, VoiceProcessor}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object App {

  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      println("Debe indicar el nombre de la aplicaci\u00f3n")
    else if (args.length == 1)
      println("Debe indicar el nombre del proceso")
    else if (args.length == 2)
      println("Debe indicar la carpeta base")
    else if (args.length == 3)
      println("Debe indicar el ambiente en que se va a ejecutar")
    else if (args.length == 4)
      println("Debe indicar el work repo...")
    else if (args.length == 5)
      println("Debe indicar la variable...")
    else if (args.length == 6)
      println("Debe indicar el job name")
    else if (args.length == 7)
      println("Debe indicar los servidores kafka")
    else if (args.length == 8)
      println("Debe indicar el t\u00f3pico de kafka")
    else {
      val APP_NAME = args(0)
      val PROCESS_TYPE = args(1)
      val FOLDER_PATH = args(2)
      val ENVIRONMENT = args(3)
      val WORK_REPO = args(4)
      val VARIABLE_NAME = args(5)
      val JOB_NAME = args(6)
      val KAFKA_SERVERS = args(7)
      val KAFKA_TOPIC = args(8)

      val NUM_FILES = if (args.length > 9) Try(args(9).toInt).getOrElse(-1) else -1

      val DATES = if (args.length > 10) args(10) else null

      val sparkConfiguration = new SparkConf().setAppName(APP_NAME)

      val ss = SparkSession.builder()
        .config(sparkConfiguration)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      val charset = "ISO-8859-1"
      val separator = "¦"

     val processor: MultiFileProcessor = PROCESS_TYPE match {
        case "VOZ" =>
          new VoiceProcessor(
            ss, ENVIRONMENT, WORK_REPO, VARIABLE_NAME, JOB_NAME, charset, FOLDER_PATH, separator,
            DATES, NUM_FILES, KAFKA_SERVERS, KAFKA_TOPIC
          )
        case "DATOS" =>
          new DataProcessor(
            ss, ENVIRONMENT, WORK_REPO, VARIABLE_NAME, JOB_NAME, charset, FOLDER_PATH, separator,
            DATES, NUM_FILES, KAFKA_SERVERS, KAFKA_TOPIC
          )
        case _ => null
      }

      if (processor != null) {
        processor.process()
      }
    }

  }

}
