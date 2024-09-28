import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PARTA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataFrame Query - Part A")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("titanic.csv")

    df.cache()

    val averageAge = df.select(avg($"Age")).as[Double].first()

    val dfWithAverageAge = df.withColumn("AverageAge", lit(averageAge))

    val olderPassengersDetails = dfWithAverageAge
      .filter($"Age" > $"AverageAge")
      .select(
        split($"Name", ",")(0).alias("LastName"),
        $"Age",
        $"AverageAge"
      )
      .distinct()
      .orderBy("LastName") 

    olderPassengersDetails.show()

    olderPassengersDetails
      .repartition(1)
      .write
      .option("header", "true")
      .csv("PARTA-output")

    spark.stop()
  }
}
