package io.github.aviatorhub.aviator.example

import io.github.aviatorhub.aviator.app.AviatorSqlApp
import org.apache.flink.table.api.TableResult

/**
 * HelloWorld example
 *
 * @author meijie
 * @since 2021/10/12
 */
object HelloWorld extends AviatorSqlApp() {

  def main(args: Array[String]): Unit = {
    runJob(args)

  }

  override def process(): TableResult = {
    createPrintTable(
      s"""
         | CREATE TABLE HELLO (
         | `message` varchar
         | )
         |""".stripMargin)

    tableEnv.executeSql(
      """
        | insert into HELLO VALUES('Hello, World')
        |""".stripMargin)
  }
}
