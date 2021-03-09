package com.revature.wetjobads

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object Runner {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
        .builder()
        .appName("WET Job Ads")
        .master("local[4]")
        .getOrCreate()

        // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
        val key = System.getenv(("AWS_ACCESS_KEY_ID"))
        val secret = System.getenv(("AWS_SECRET_ACCESS_KEY"))

        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

        val sc = spark.sparkContext

        import spark.implicits._
        sc.setLogLevel("WARN")

        val wetSegments = "s3a://jan-2021-test-txt-bucket/test.txt"
        val wetS3Paths = sc
        .textFile(wetSegments)
        .map("s3a://commoncrawl/" + _)
        .collect()
    
        // textFile() will take a string of comma separated S3 URIs and read them
        // into one big RDD
        val inputFiles = wetS3Paths.mkString(",")

        // prints the contents of every WET file segment 
        // better not do this with too many segments locally
        // wetData.foreach(println)

        val delimiter = "WARC/1.0"
        val conf = new Configuration(sc.hadoopConfiguration)
        conf.set("textinputformat.record.delimiter", delimiter)
        val hadoopFile = sc.newAPIHadoopFile(
        inputFiles,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text],
        conf
        )

        val records = hadoopFile.map { case (longWritable, text) => text.toString }
        val jobAdsRdd = findJobAds(records)
        val jobAdsDf = jobAdsRdd.take(1000).foreach(println)
    
    }

    def findJobAds(records: RDD[String]): RDD[String] = {
        records
        .filter(record => {
            val lines = record.split("\n")
            val containsJobUri = lines
            .find(_.startsWith("WARC-Target-URI:"))
            .map(uriHeader => uriHeader.split(" "))
            .map(split => if (split.length == 2) split(1) else "")
            .map(uri => uri.contains("job"))

            containsJobUri.getOrElse(false)
      })
      .map(record => {
        val lines = record.split("\n")
        val textWithoutHeaders = lines.filter(l => {
          !l.startsWith("WARC") &&
            !l.startsWith("Content-Type:") &&
            !l.startsWith("Content-Length:") &&
            !l.trim().isEmpty()
        })
        textWithoutHeaders.mkString("")
      }).filter(ad => {
            val lowercase = ad.toLowerCase()
            lowercase.contains("frontend") ||
            lowercase.contains("backend") ||
            lowercase.contains("fullstack") ||
            lowercase.contains("cybersecurity") ||
            lowercase.contains("software") ||
            lowercase.contains("computer")
      }).flatMap(line => line.split(" "))
  }
  
}
