import org.apache.spark.sql.SparkSession

object PARTD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple Mean Age Comparison of Titanic Passengers")
      .getOrCreate()

    val df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("titanic.csv")

    df.createOrReplaceTempView("titanic")

    val avgAgeSurvivors = spark.sql("""
      SELECT AVG(Age) as avg_age_survivors
      FROM titanic
      WHERE Survived = 1
    """).collect().head.getDouble(0) 

    val avgAgeNonSurvivors = spark.sql("""
      SELECT AVG(Age) as avg_age_non_survivors
      FROM titanic
      WHERE Survived = 0
    """).collect().head.getDouble(0) 

    println(s"Average age of survivors: $avgAgeSurvivors")
    println(s"Average age of non-survivors: $avgAgeNonSurvivors")

    spark.stop()
  }
}

