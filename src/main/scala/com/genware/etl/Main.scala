package com.genware.etl

import cats.data._
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

case object Empty
case class ProcessingKeys(keys: List[String])
case class ProcessingDF(df: DataFrame)
case class Done(df: DataFrame)


object Main extends IOApp{
  implicit val session = (SparkSession
    .builder()
    .appName("Retaining Sanity with Spark")
    .master("local")
    .getOrCreate())

  import session.implicits._

  def loadKeys1 = IO{ProcessingKeys(List.fill(10)(Random.nextString(5)))}
  def loadKeys2 = IO{ProcessingKeys(List.fill(1000)(Random.nextString(10)))}

  def loadKeys: IndexedStateT[IO, Empty.type, List[ProcessingKeys], Unit] = IndexedStateT.setF {
      List(loadKeys1, loadKeys2).parSequence
  }

  def pruneSomeKeys: StateT[IO, List[ProcessingKeys], Unit] =
    StateT.modifyF { s =>
      s.map(ss=>IO(ss.copy(keys = ss.keys drop 1))).sequence
    }

  def pruneMoreKeys: StateT[IO, List[ProcessingKeys], Unit] =
    StateT.modifyF { s =>
      s.map(ss=>IO(ss.copy(keys = ss.keys drop 1))).sequence
    }

  def createDF(implicit spark: SparkSession): IndexedStateT[IO, List[ProcessingKeys], List[ProcessingDF], Unit] =
    IndexedStateT.modifyF { s =>
      s.map(ss=>IO(ProcessingDF(ss.keys.toDF))).sequence
    }

  def transformDF(implicit spark: SparkSession): IndexedStateT[IO, List[ProcessingDF], Done, Unit] =
    IndexedStateT.modifyF { s =>
      IO(Done(s.head.df limit 3))
    }

  val indexedStateProgram: IndexedStateT[IO, Empty.type, Done, Unit] = for {
    _ <- loadKeys
    _ <- pruneSomeKeys
    _ <- pruneMoreKeys
    _ <- createDF
    _ <- transformDF
  } yield ()

  override def run(args: List[String]): IO[ExitCode] =
       for{
       _ <- indexedStateProgram.run(Empty)
       }yield (ExitCode.Success)
}