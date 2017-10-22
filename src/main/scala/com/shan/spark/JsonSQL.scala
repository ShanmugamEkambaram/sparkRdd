import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object sqlspark {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val df = spark.read.format("json").json("src/main/resources/companies.json")
    df.printSchema()
    // root

    // Creates a temporary view using the DataFrame
    df.createOrReplaceTempView("companies")

    // SQL statements can be run by using the sql methods provided by spark
    val compNameDF = spark.sql("SELECT name,founded_year FROM companies WHERE number_of_employees  BETWEEN 47 AND 54")
    df.groupBy("founded_year").max("number_of_employees").show
    compNameDF.show()

    //df.show()
  }
}