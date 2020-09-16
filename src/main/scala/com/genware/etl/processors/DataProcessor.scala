package com.genware.etl.processors

import cats.effect.Sync
import com.genware.etl.common.ContextExecutor
import com.genware.etl.models.ErrorInfo

trait DataProcessor {
  def process(context: ContextExecutor): Unit
}

object DataProcessor {
  def apply(config: Map[String, Any]): Either[ErrorInfo, DataProcessor] = config.get("type") match {
    case Some("sql") => SqlProcessor(config.get("text"), config.get("outputDf"), config.get("outputAlias"))
    case _ => Left(ErrorInfo("unknown data processor type"))
  }
}
