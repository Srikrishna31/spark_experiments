/**
  * Code from this site: https://mapr.com/blog/spark-streaming-and-twitter-sentiment-analysis/
  *
  */

import java.sql.Timestamp
import org.apache.spark._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{write, read}
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

case class Tweet(text:String, user:String, id:Long, createdAt:Timestamp,
                 retweetCount:Int, location:String, language:String)

object TwitterStream extends App {

  if (args.length < 4) {
    println("Usage: TwitterStream.main('consumerKey', 'consumerSecret', 'accessToken', 'accessTokenSecret') \n " +
      "replace the key and secret keys with appropriate twitter hash values.")
  }
//  else {
    val config = new SparkConf().setAppName("twitter-stream-sentiment-krishna")
      .setMaster("local[3]")
      .set("spark.executor.memory", "1g")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val s = new StreamingContext(sc, Seconds(2))
    //  s.checkpoint("/media/sf_D_DRIVE/TwitterStreamData/")
    s.checkpoint("D:/TwitterStreamData/")

      System.setProperty("twitter4j.oauth.consumerKey", args(0))
      System.setProperty("twitter4j.oauth.consumerSecret", args(1))
      System.setProperty("twitter4j.oauth.accessToken", args(2))
      System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))

    val stream = TwitterUtils.createStream(s, None)
    stream.count.print

    implicit val formats = Serialization.formats(NoTypeHints)

    val formatter = DateTimeFormatter.ofPattern("dd_MM_yyyy-HH_mm_ss")

    stream.map(s => Tweet(s.getText, s.getUser.getName,
      s.getId, new Timestamp(s.getCreatedAt.getTime),
      s.getRetweetCount, s.getUser.getLocation, s.getLang))
      .map(write(_))
      .reduce(_+ "\n"+ _)
      .foreachRDD{s =>
        val now = LocalDateTime.now
        Files.write(Paths.get(s"D:/TwitterStreamData/${formatter.format(now)}.json"),
          s.collect.foldLeft("")((z, a) => z+a+"\n")
            .getBytes(StandardCharsets.UTF_8)) }
    s.start
    s.awaitTermination
//  }

}
