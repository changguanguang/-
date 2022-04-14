package request5

import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("assd")
    val sc = new SparkContext(sparkConf)


    sc.stop()

  }

}
