package com.genware.etl.sources

import cats.effect.Sync
import scala.collection.Map
import com.genware.etl.common.ContextExecutor
import com.genware.etl.models.{ConfigParam, ErrorInfo}

trait DataSource {
  def load[F[_]: Sync](context: ContextExecutor): F[Unit]
  def applyTemplate(config: ConfigParam): DataSource
}

object DataSource {
  def apply(config: Map[String, _]): Either[ErrorInfo, DataSource] = config.get("type") match {
    case Some("hdfs") => HdfsLoader(config.get("path"), config.get("outputAlias"), config.get("outputDf"), config.get("format"), config.get("options"), config.get("columns"))
    case _ => Left(ErrorInfo("Unknown data source type"))
  }
}
