import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object problem4 {
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("problem4")
    conf.setMaster("local")
    val spark_context=new SparkContext(conf)

    val spark=SparkSession
      .builder()
      .appName("problem4")
      .config("test","test")
      .getOrCreate()

    import spark.implicits._

    my_MMM(spark)
  }

  def my_MMM(spark: SparkSession): Unit =
  {
    import spark.implicits._

    val matrix_schema=new StructType()
      .add("i",IntegerType,true)
      .add("j",IntegerType,true)
      .add("val",IntegerType,true)

    val matrix_schema1=new StructType()
      .add("i1",IntegerType,true)
      .add("j1",IntegerType,true)
      .add("val1",IntegerType,true)

    val M1=spark.read
      .format("csv")
      .option("header","false")
      .schema(matrix_schema)
      .load("input/M1.csv")

    //M1.show()

    val M2=spark.read
      .format("csv")
      .option("header","false")
      .schema(matrix_schema1)
      .load("input/M2.csv")

    //M2.show()

    //1st map
    val map_1=M1
      .join(M2,M1("j")===M2("i1"))

    //map_1.show()

    //1st reduce
    val reduce_1=map_1
      .withColumn("v",map_1("val")*map_1("val1"))
      //.select(concat($"i", lit(","), $"j1"))
      .select("i","j1","v")

    //reduce_1.show()

    //2nd map and reduce
    val ans=reduce_1
      .groupBy(reduce_1("i"),reduce_1("j1"))
      .agg(sum("v"))
      .sort("i")

    //ans.show()

    //write to file
    ans
      .repartition(1)
      .write.format("csv")
      .option("header","false")
      .save("my_MMM_ans.csv")
  }
}
