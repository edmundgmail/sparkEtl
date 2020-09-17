package com.genware.etl.common

import java.io.{File, FileInputStream}

import cats.effect._
import cats.implicits._
import com.genware.etl.models.{ConfigParam, ErrorInfo}
import com.genware.etl.processors.DataProcessor
import com.genware.etl.sinks.DataSink
import com.genware.etl.sources.DataSource
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml
import scala.collection.parallel.mutable

trait Environment {
  private def getYamlConfig(path: String): Either[ErrorInfo, Map[String, _]] = Utils.toScalaTypeT[Map[String, _]](new Yaml().load(new FileInputStream(new File(path))))
  private def getSparkConf(config: Option[Any]): Either[ErrorInfo, SparkConf] = config match {
    case Some(conf) => Utils.toT[List[Map[String, String]]](conf).map(s => s.reduce(_ ++ _)).map(new SparkConf().setAll(_))
    case _ => Left(ErrorInfo("Can't load spark conf"))
  }

  private def getEsConf(config: Option[Any])(sparkConf: SparkConf): Either[ErrorInfo, SparkConf] = config match {
    case Some(conf) => Utils.toT[List[Map[String, Any]]](conf).map(s => s.reduce(_ ++ _)).map(confs=>Utils.translateEsConf(confs).map(sparkConf.setAll(_)).getOrElse(sparkConf))
    case _ => Left(ErrorInfo("Can't load Es conf "))
  }

  def initSparkSession(config: ConfigParam) : Either[ErrorInfo, SparkSession] =
    for {
      commonConf <- getYamlConfig(config.appYml)
      sparkConf <- getSparkConf(commonConf.get("spark"))
      sparkConf <- getEsConf(commonConf.get(s"elastic-${config.env}"))(sparkConf)
    }yield SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate

  private def getSources(config: Option[Any]): Either[ErrorInfo, List[DataSource]] = config match {
    case Some(conf) => {
     val r = for {
        cf <- Utils.toT[List[Map[String, _]]](conf)
        c <- cf.map(s => DataSource(s)).sequence
      } yield c
      r
    }
    case _ => Left(ErrorInfo("Can't get the sources"))
  }

  private def getSinks(config: Option[Any]): Either[ErrorInfo, List[DataSink]] = config match {
    case Some(conf) => for {
      cf <- Utils.toT[List[Map[String, _]]](conf)
      c <- cf.map(s=>DataSink(s)).sequence
    }yield c

    case _ => Left(ErrorInfo("Can't get the Sinks"))
  }

  private def getProcessors(config: Option[Any]): Either[ErrorInfo, List[DataProcessor]] = config match {
    case Some(conf) => for {
      cf <- Utils.toT[List[Map[String, _]]](conf)
      c <- cf.map(s=>DataProcessor(s)).sequence
    } yield c
    case _ => Left(ErrorInfo("Can't load the processors"))
  }

  def setup(config: ConfigParam, sparkSession: SparkSession) : Either[ErrorInfo, ContextExecutor] = for {
    jobConf <- getYamlConfig(config.jobYml)
    sources <- getSources(jobConf.get("sources"))
    sinks <- getSinks(jobConf.get("sinks"))
    processors <- getProcessors(jobConf.get("processors"))
  }yield new ContextExecutor(sparkSession, sources, sinks, processors, mutable.ParHashMap.empty[String, DataFrame])

  def parseParam(args: List[String]) : Either[ErrorInfo, ConfigParam] =
    if(args.length < 3)
      Left(ErrorInfo("Usage: program appYml jobYml environment \n Example: Main application.yml job1.yml dev\n"))
    else
      Right(ConfigParam(args(0), args(1), args(2)))
}
