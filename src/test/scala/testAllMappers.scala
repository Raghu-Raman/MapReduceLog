import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*

import collection.JavaConverters.asScalaIteratorConverter
import java.io.{File, FileOutputStream, IOException}
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.*
import org.joda.time.LocalTime

object testAllMappers:
  def main(args: Array[String]): Unit = {
  // The first test case will test all the success cases with string containing one log message of each type with time limit falling between the given interval
  val testPattern1 ="14:27:31.351 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - #$%))ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3(-+]\n14:28:22.508 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - {@%^))ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u(:\n14:29:27.815 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - %%((^$J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r_+=\n14:31:04.264 [scala-execution-context-global-14] WARN  HelperUtils.Parameters$ - ~?}{|bg0cg3ce1H7wbf1ag2af3ce2zxmp\n"
  // The second test case will fail with all the log message being out of the given time interval . NOTE: The message is same as testPattern1 with hour value changed from 14 to 09
  val testPattern2 = "09:27:31.351 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - #$%))ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3(-+]\n09:28:22.508 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - {@%^))ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u(:\n09:29:27.815 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - %%((^$J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r_+=\n09:31:04.264 [scala-execution-context-global-14] WARN  HelperUtils.Parameters$ - ~?}{|bg0cg3ce1H7wbf1ag2af3ce2zxmp\n"
  // The following test pattern will be a test case for the second job with alternating succcess and failure in single string
  val testPattern3 = "09:27:31.351 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - #$%))ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3(-+]\n14:28:22.508 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - {@%^))ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u(:\n14:29:27.815 [scala-execution-context-global-14] ERROR HelperUtils.Parameters$ - %%((^$J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r_+=\n09:31:04.264 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - ~?}{|bg0cg3ce1H7wbf1ag2af3ce2zxmp\n"
  // The following will test the total number of log messges with the injected string pattern. It is a combination of testPattern1 and testPattern2
  val testPattern4 = "14:27:31.351 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - #$%))ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3(-+]\n14:28:22.508 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - {@%^))ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u(:\n14:29:27.815 [scala-execution-context-global-14] WARN HelperUtils.Parameters$ - %%((^$J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r_+=\n14:31:04.264 [scala-execution-context-global-14] DEBUG  HelperUtils.Parameters$ - ~?}{|bg0cg3ce1H7wbf1ag2af3ce2zxmp\n09:27:31.351 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - #$%))ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3(-+]\n09:28:22.508 [scala-execution-context-global-14] ERROR  HelperUtils.Parameters$ - {@%^))ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u(:\n09:29:27.815 [scala-execution-context-global-14] DEBUG HelperUtils.Parameters$ - %%((^$J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r_+=\n09:31:04.264 [scala-execution-context-global-14] WARN  HelperUtils.Parameters$ - ~?}{|bg0cg3ce1H7wbf1ag2af3ce2zxmp\n"

  val configTest = ConfigFactory.load()
  val startTimei = configTest.getString("MR.startTime") // MR represents the config unique to job 1. Start time of the interval is obtained
  val endTimei = configTest.getString("MR.endTime") // End time of the interval is obtained
  val startTime = LocalTime.parse(startTimei)
  val endTime = LocalTime.parse(endTimei)
  val timeRegex = configTest.getString("commonMR.timeRegex").r // Time regex is obtained to find the time in the given log interval
  val logRegex = configTest.getString("commonMR.logRegex").r // Log regex is used to find the log (INFO, ERROR, WARN, DEBUG ) from the given message
  val patternRegex = configTest.getString("commonMR.patternRegex").r // Pattern Regex is used to find the occurrence of the pattern in the given log message.
  val errorRegex = configTest.getString("MR1.errorRegex").r
  var infoCount: Int = 0
  var warnCount: Int = 0
  var errorCount: Int = 0
  var debugCount: Int = 0
  testPattern1.split("\n").foreach{token =>
    val timeValue = timeRegex.findFirstIn(token).get
    val timeParse = LocalTime.parse(timeValue)
    val patternValue = patternRegex.findFirstIn(token).get
    val logValue = logRegex.findFirstIn(token).get

    if(timeParse.isAfter(startTime)&&timeParse.isBefore(endTime))
    {
      if(timeValue == "14:27:31"&& patternValue == "ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3" && logValue =="INFO"){
          infoCount += 1
        }

      if (timeValue == "14:28:22" && patternValue == "ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u" && logValue == "ERROR") {
        errorCount += 1
      }
      if (timeValue == "14:29:27" && patternValue == "J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r" && logValue == "DEBUG") {
        debugCount += 1
      }

      if (timeValue == "14:31:04" && patternValue == "bg0cg3ce1H7wbf1ag2af3ce2" && logValue == "WARN") {
        warnCount += 1
      }
    }
    if (infoCount == 1 && warnCount == 1 && debugCount == 1 && errorCount == 1) {
      println("Success: All test cases passed")
      infoCount = 0
      warnCount = 0
      debugCount = 0
      errorCount = 0
    }
  }
    testPattern2.split("\n").foreach { token =>
      val timeValue = timeRegex.findFirstIn(token).get
      val timeParse = LocalTime.parse(timeValue)
      val patternValue = patternRegex.findFirstIn(token).get
      val logValue = logRegex.findFirstIn(token).get
      var infoCount: Int = 0
      var warnCount: Int = 0
      var errorCount: Int = 0
      var debugCount: Int = 0
      if (timeParse.isAfter(startTime) && timeParse.isBefore(endTime)) {
        if (timeValue == "14:27:31" && patternValue == "ae1ce2cf1C8vbe2af1bf0E5jE9rP9faf1cf2L7fN9fbg3" && logValue == "INFO") {
          infoCount += 1
        }

        if (timeValue == "14:28:22" && patternValue == "ae1D6ibf1ce1ce3ag2D8rK9lL8maf1K8kK9u" && logValue == "ERROR") {
          errorCount += 1
        }
        if (timeValue == "14:29:27" && patternValue == "J9mY9uag1bf2P5fP8uC8qC5mZ9tbe1R8r" && logValue == "DEBUG") {
          debugCount += 1
        }

        if (timeValue == "14:31:04" && patternValue == "bg0cg3ce1H7wbf1ag2af3ce2" && logValue == "WARN") {
          warnCount += 1
        }
      }
      if (infoCount == 1 || warnCount == 1 || debugCount == 1 || errorCount == 1) {
        println("Failure : A test case failed")
        errorCount = 0
      }
    }
    testPattern3.split("\n").foreach { token =>
      val timeValue = timeRegex.findFirstIn(token).get
      val timeParse = LocalTime.parse(timeValue)
      val patternValue = patternRegex.findFirstIn(token).get
      val errorValue = errorRegex.findFirstIn(token).get
      if (timeParse.isAfter(startTime) && timeParse.isBefore(endTime)) {
        if (timeParse.isAfter(startTime) && timeParse.isBefore(endTime) && errorValue == "ERROR") {
          errorCount += 1
        }
        }
    }
    if (errorCount == 2) {
      println("Success: Test cases for error Regex passed")
    }
    else {
      println("Failure : A test case failed for Error Job.")
    }
    warnCount = 0
    infoCount = 0
    debugCount = 0
    errorCount = 0
    testPattern4.split("\n").foreach { token =>
      val timeValue = timeRegex.findFirstIn(token).get
      val logValue = logRegex.findFirstIn(token).get
        if (logValue == "INFO") {
          infoCount += 1
        }
        if (logValue == "ERROR") {
          errorCount += 1
        }
        if (logValue == "DEBUG") {
          debugCount += 1
        }
        if (logValue == "WARN") {
          warnCount += 1
        }
      }
    val totalCount = infoCount+errorCount+debugCount+warnCount
    if(totalCount==8){
      println("Success: The summation of all the log message occurrences passes the test")
    }
    else{
      println("Failure: The summation of all the log message occurrences aren't equal to the actual length")
    }

    warnCount = 0
    infoCount = 0
    debugCount = 0
    errorCount = 0
    val infoLength = 54
    val errorLength = 44
    val debugLength = 42
    val warnLength = 33
    testPattern1.split("\n").foreach(token=>
      val logFound = logRegex.findFirstIn(token)
      if(logFound!=None){
        if(logFound.get=="INFO"){
          infoCount = token.split(" ").last.length
        }
        if (logFound.get == "ERROR") {
          errorCount = token.split(" ").last.length
        }
        if (logFound.get == "DEBUG") {
          debugCount = token.split(" ").last.length
        }
        if (logFound.get == "WARN") {
          warnCount = token.split(" ").last.length
        }
      }
    )
    if(infoLength==infoCount && warnLength==warnCount && debugLength==debugCount && errorLength==errorCount ){
      println("Success: The length of all the patterns matches withe test case.")
    }
    else{
      println("Failure : The length doesn't match with the test case")
    }
  }
