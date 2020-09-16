package com.genware.etl.common

import cats.effect.Sync
import com.genware.etl.processors.DataProcessor
import com.genware.etl.sinks.DataSink
import com.genware.etl.sources.DataSource
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.mutable

case class ContextExecutor(spark: SparkSession, sources: List[DataSource], sinks: List[DataSink], processors: List[DataProcessor], df: mutable.ParHashMap[String, DataFrame]) {
  def execute[F[_]: Sync]: F[Unit] = Sync[F].pure(Unit)
}
