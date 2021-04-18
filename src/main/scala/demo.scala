import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object demo {

  def main(array: Array[String]): Unit = {
    read_csv
  }

  def read_csv(): Unit = {
    val conf = new SparkConf().setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    val df = sc.parallelize(Seq(1,2,3,4,5)).toDF()
    df.explain(true)
    df.printSchema()
    df.count()
  }
}
