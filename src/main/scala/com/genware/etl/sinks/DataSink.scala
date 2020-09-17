package com.genware.etl.sinks

import cats.effect.Sync
import com.genware.etl.common.ContextExecutor
import com.genware.etl.models.{ConfigParam, ErrorInfo}

trait DataSink {
  def applyTempalte(config: ConfigParam) : DataSink
  def write[F[_]: Sync](context: ContextExecutor) : F[Unit]
}

object DataSink {
  def apply(config: Map[String, _]) : Either[ErrorInfo, DataSink] = config.get("type") match {
    case Some("hdfs") => HdfsWriter(config.get("disabled"), config.get("persiteDf"), config.get("format"), config.get("target"), config.get("options"), config.get("partitionby"))
    case _ => Left(ErrorInfo("Can't construct DataSink, unnkown type"))
  }
}
