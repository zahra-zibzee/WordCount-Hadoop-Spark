import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PARTB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataFrame Query - Part B")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("titanic.csv")

    df.cache() 

    df.createOrReplaceTempView("titanic")

    val averageAgeDF = spark.sql("SELECT AVG(Age) as average_age FROM titanic")
    averageAgeDF.createOrReplaceTempView("average_age_view")

    val query = """
      SELECT DISTINCT split(t.Name, ',')[0] AS LastName, t.Age, avg.average_age
      FROM titanic t, average_age_view avg
      WHERE t.Age > avg.average_age
      ORDER BY LastName
    """

    val result = spark.sql(query)

    result.show()

    result.coalesce(1)
      .write
      .option("header", "true")
      .csv("PARTB-output")

    spark.stop() 
  }
}
