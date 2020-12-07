import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object problem2 {

  def main(args: Array[String]) {
  {
    /*
    val conf = new SparkConf().setAppName("problem2")
    conf.setMaster("local")
    val spark_context=new SparkContext(conf)

    val spark=SparkSession
      .builder()
      .appName("problem2")
      .config("test","test")
      .getOrCreate()*/

    val spark=SparkSession
      .builder()
      .appName("problem2")
      .config("spark.master","local")
      .getOrCreate()

    import spark.implicits._

    query_action(spark)

  }
  def query_action(spark: SparkSession): Unit =
    {
      val cus_schema=new StructType()
        .add("ID",IntegerType,true)
        .add("Name",StringType,true)
        .add("Age",IntegerType,true)
        .add("Gender",StringType,true)
        .add("CountryCode",IntegerType,true)
        .add("Salary",FloatType,true)

      val tran_schema=new StructType()
        .add("TransID",IntegerType,true)
        .add("CustID",IntegerType,true)
        .add("TransTotal",FloatType,true)
        .add("TransNumItems",IntegerType,true)
        .add("TransDesc",StringType,true)

      val cus_file=spark.read
        .format("csv")
        .option("header","false")
        .schema(cus_schema)
        .load("input/test_cus.csv")

      //cus_file.printSchema()
      //cus_file.select("name").show()

      val tran_file=spark.read
        .format("csv")
        .option("header","false")
        .schema(tran_schema)
        .load("input/test_tran.csv")


      val joined_table=cus_file
        .join(tran_file,cus_file("ID")===tran_file("CustID"))
        .select("Gender","CountryCode","TransTotal")

      //joined_table.show()

      val temp=joined_table
        .groupBy(joined_table("CountryCode"))
        .agg(sum("TransTotal"))

      //temp.show()

      val temp2=temp
        .agg(max("sum(TransTotal)"))

      //temp2.show()

      val country_code=temp
        .join(temp2,temp("sum(TransTotal)")===temp2("max(sum(TransTotal))"))
        .select("CountryCode")

      //country_code.show()
      val temp_ans=country_code
        .join(cus_file,country_code("CountryCode")===cus_file("CountryCode"))
        .select("ID","Gender")

      //temp_ans.show()

      val ans=temp_ans
        .groupBy(temp_ans("Gender"))
        .agg(count("ID"))

      ans.show()
    }
  }
}
