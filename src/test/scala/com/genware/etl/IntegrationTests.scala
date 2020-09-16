package com.genware.etl

import cats.effect._
import com.genware.etl.common.Environment
import com.genware.etl.models.ConfigParam

class IntegrationTests extends Testing with Environment{
  val configParam = ConfigParam("src/test/resources/test.yml", "src/test/resource/job1.yml", "dev")
  val sparkSessionF = initSparkSession(configParam)
  test("Test the read hdfs, write hdfs") {
    val r = for{
      sparkSession <- initSparkSession(configParam)
      ctx <- setup(configParam, sparkSession)
    }yield (ctx.execute[IO])

    r.map{ p =>
      p.handleErrorWith {
        case e => IO(e.printStackTrace)
      }.map(_ => ExitCode.Error)
    }.getOrElse(IO.pure(ExitCode.Error))
  }

  override protected def afterAll(): Unit = {
    print("goodby")
  }
}
