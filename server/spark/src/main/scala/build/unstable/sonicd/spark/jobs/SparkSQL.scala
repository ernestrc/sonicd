package build.unstable.sonicd.spark.jobs

import build.unstable.sonicd.model.{DoneWithQueryExecution, OutputChunk}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL extends App {

  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null

  try {

    val master = sys.env.getOrElse("SPARK_MASTER", throw new Exception("missing 'SPARK_MASTER' environment in spark job"))
    val query = sys.env.getOrElse("QUERY", throw new Exception("missing 'SPARK_MASTER' environment in spark job"))
    val queryId = sys.env.getOrElse("QUERY_ID", throw new Exception("missing 'SPARK_MASTER' environment in spark job"))
    val columnNames = sys.env.getOrElse("PRINT_COLUMN_NAMES", throw new Exception("missing 'SPARK_MASTER' environment in spark job"))

    val appName = "sonicd_" + queryId.take(5)

    sparkContext = {
      val conf = new SparkConf()
        .setAppName(appName)
        .setMaster(master)

      new SparkContext(conf)
    }

    sqlContext = new SQLContext(sparkContext)

    val df = sqlContext.sql(query)

    // FIXME
    //df.foreach(r ⇒ println(OutputChunk(r.toSeq.map(t ⇒ t.toString).toVector).json.compactPrint))

    println(DoneWithQueryExecution(success = true))

    sparkContext.stop()
  } catch {
    case e: Exception ⇒
      println(DoneWithQueryExecution.error(e).json.compactPrint)
      if (sparkContext != null && !sparkContext.isStopped)
        sparkContext.stop()
      System.exit(1)

  }
}
