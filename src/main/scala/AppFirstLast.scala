import com.claro.Cdrs.core.FirstLastProcessor
import com.claro.Cdrs.processors.DataVoiceProcessor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppFirstLast {

  def main(args: Array[String]): Unit = {
    if (args.length == 0)
      println("Debe indicar el nombre de la aplicaci\u00f3n")
    else if (args.length == 1)
      println("Debe indicar los servidores kafka")
    else if (args.length == 2)
      println("Debe indicar el t\u00f3pico de kafka")
    else {
      val APP_NAME = args(0)
      val KAFKA_SERVERS = args(1)
      val KAFKA_TOPIC = args(2)

      val sparkConfiguration = new SparkConf().setAppName(APP_NAME)

      val ss = SparkSession.builder()
        .config(sparkConfiguration)
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .getOrCreate()

      val charset = "ISO-8859-1"
      val separator = "¦"

      val processor: FirstLastProcessor = new DataVoiceProcessor(
            ss, KAFKA_SERVERS, KAFKA_TOPIC
          )

      if (processor != null) {
        processor.process()
      }
    }

  }
}
