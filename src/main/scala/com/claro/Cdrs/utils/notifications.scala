package com.claro.Cdrs.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.sys.process._

import scala.util.Try

class notifications {

  def log(message: String): Unit = {
    val df = new SimpleDateFormat("yyyyMMdd' 'HH:mm")
    val calendar = Calendar.getInstance()
    val dateLog = df.format(calendar.getTime)
    println("Log_" + dateLog + ": " + message)
  }

  def inicio_proceso(path_ejecuta_escenario: String, repo_sisnot: String, name_job: String): Unit = {
    Try {
      val notification = "java -jar " + path_ejecuta_escenario + " " + repo_sisnot + " SC_JH_SISNOT_INICIO_PROCESO" + " " + name_job
      log(notification)
      notification.!
    }
  }

  def error_proceso(path_ejecuta_escenario: String, repo_sisnot: String, name_job: String, mensaje: String): Unit = {
    Try {
      log(mensaje)
      val msj_sisnot= mensaje.replace(" ", "_")
      val notification = "java -jar " + path_ejecuta_escenario + " " + repo_sisnot + " SC_JH_SISNOT_ERROR" + " " + name_job + " " + msj_sisnot
      log(notification)
      notification.!
    }
  }

  def fin_proceso(path_ejecuta_escenario: String, repo_sisnot: String, name_job: String): Unit = {
    Try {
      val notification = "java -jar " + path_ejecuta_escenario + " " + repo_sisnot + " SC_JH_SISNOT_FIN_PROCESO" + " " + name_job
      log(notification)
      notification.!
    }
  }

}
