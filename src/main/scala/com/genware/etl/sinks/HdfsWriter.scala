package com.genware.etl.sinks
import cats.effect._
import cats.implicits._
import com.genware.etl.common.{ContextExecutor, Utils}
import com.genware.etl.models.{ConfigParam, ErrorInfo}
import org.apache.spark.sql.{DataFrame, SaveMode}

case class HdfsWriter(disabled: Boolean, outputDf: String, format: String, target: String, options: List[(String, String)], partitionBy: Option[String]) extends DataSink {
  override def applyTempalte(config: ConfigParam): HdfsWriter = this

  override def write[F[+_] : Sync](context: ContextExecutor): F[Unit] = {
    val df = context.df.get(outputDf)
    if(!disabled && df.isDefined) {
       Sync[F].delay {
        val partitions = partitionBy.map(_.split(",").toList).getOrElse(List())
        val df:DataFrame = context.df.get(outputDf).getOrElse(context.spark.emptyDataFrame)
        options.foldLeft(partitions.foldLeft(df.write)((z, b)=>z.partitionBy(b)))((z, o) => z.option(o._1, o._2)).mode(SaveMode.Overwrite).save(target)
      }
    }else{
      Sync[F].unit
    }
  }

}

object HdfsWriter {
  def apply(disabled: Option[Any], outputDf: Option[Any], format: Option[Any], target: Option[Any], options: Option[Any], partitionby: Option[Any]): Either[ErrorInfo, HdfsWriter] = {
    (outputDf, format, target) match {
      case (Some(d), Some(f), Some(t)) => Utils.toListMapWithOption(options).map(oo => new HdfsWriter(Utils.toOptionBoolean(disabled), d.toString, f.toString, t.toString, oo, partitionby.map(_.toString)))
      case _ => Left(ErrorInfo("can't construct Hdfs writer"))
    }
  }
}

