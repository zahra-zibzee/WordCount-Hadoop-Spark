import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.util.matching.Regex


object SparkWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkWordCount <input-directory> <output-directory>")
      System.exit(1)
    }

    val wordRegex = new Regex("""[a-z]{5,25}""")
    val numRegex = new Regex("""[0-9]{2,12}""")

    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    val textFile = ctx.textFile(args(0))

    val counts = textFile
                 .flatMap(line => line.toLowerCase.split(" ")) // a
                 .filter(word => wordRegex.pattern.matcher(word).matches || numRegex.pattern.matcher(word).matches) // a
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
                 .coalesce(1)       // only one output

    counts.saveAsTextFile(args(1))
    ctx.stop()
  }
}

