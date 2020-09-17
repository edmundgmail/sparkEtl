package com.genware.etl

import cats.effect._
import com.genware.etl.common.Environment
import com.genware.etl.models.ConfigParam

class IntegrationTests extends Testing with Environment{
  val configParam = ConfigParam("src/test/resources/test.yml", "src/test/resources/job1.yml", "dev")

  test("Test the read hdfs, write hdfs") {
    val r = for{
      sparkSession <- initSparkSession(configParam)
      ctx <- setup(configParam, sparkSession)
    }yield (ctx.execute[IO])

    r.map{ p =>
      p.handleErrorWith {
        case e => IO(e.printStackTrace)
      }.map(_ => ExitCode.Error)
      p.unsafeRunSync
    }.getOrElse {
        println(r.left.get.info)
    }
  }

  override protected def afterAll(): Unit = {
    print("goodby")
  }
}
