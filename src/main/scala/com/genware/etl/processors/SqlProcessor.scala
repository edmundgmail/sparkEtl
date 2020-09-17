package com.genware.etl.processors

import cats.effect.Sync
import com.genware.etl.common.ContextExecutor
import com.genware.etl.models.ErrorInfo

case class SqlProcessor(sql: String, outputDf: Option[String], outputAlias: Option[String]) extends DataProcessor {
  override def process[F[_]: Sync](context: ContextExecutor): F[Unit] = Sync[F].delay {
    val df = context.spark.sql(sql)
    outputDf match {
      case Some(d) => context.df.put(d.toString, df)
      case _ =>
    }

    outputAlias match {
      case Some(a) => df.createOrReplaceGlobalTempView(a.toString)
      case _ =>
    }
  }
}

object SqlProcessor {
  def apply(sql: Option[Any], outputDf: Option[Any], outputAlias: Option[Any]): Either[ErrorInfo, SqlProcessor] = sql match {
    case Some(s) => Right(new SqlProcessor(s.toString, outputDf.map(_.toString), outputAlias.map(_.toString)))
    case _ => Left(ErrorInfo("sql text is a must for sqlprocessor"))
  }
}