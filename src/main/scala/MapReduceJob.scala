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
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

object MapReduceJob :

  val logger: Logger = LoggerFactory.getLogger(getClass)
  // JOB-1 Description : Identify the number of occurrences with injected pattern for each type of log message
  // The first Map-Reduce combo deals with performing the job one to identify the patterns in the log messages

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val word = new Text()
    //Loading up the settings for the map reduce job 1 - finding the pattern occurrences in the given time interval.
    private val configMR = ConfigFactory.load()
    private val startTimei = configMR.getString("MR.startTime") // MR represents the config unique to job 1. Start time of the interval is obtained
    private val endTimei = configMR.getString("MR.endTime") // End time of the interval is obtained
    private val timeRegex = configMR.getString("commonMR.timeRegex").r // Time regex is obtained to find the time in the given log interval
    private val logRegex = configMR.getString("commonMR.logRegex").r // Log regex is used to find the log (INFO, ERROR, WARN, DEBUG ) from the given message
    private val patternRegex = configMR.getString("commonMR.patternRegex").r // Pattern Regex is used to find the occurrence of the pattern in the given log message.
    private val startTime = LocalTime.parse(startTimei)
    private val endTime = LocalTime.parse(endTimei)


    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      println(line)
      // Iterate through each line from the file by splitting the file into lines with '\n' as the separator
      logger.info("Mapper  for the Job-1 Starting")
      line.split("\n").foreach { token =>
        val timeFound = timeRegex.findFirstIn(token).get
        val currTime = LocalTime.parse(timeFound)
        val logFound = logRegex.findFirstIn(token)
        if (currTime.isAfter(startTime) && currTime.isBefore(endTime)) {
          val patternFound = patternRegex.findFirstIn(token) // findFirst returns Some(identified string) or None
          // Check if the return value is not None
          if (patternFound != None) {
            val temp = logFound.get
            word.set(temp)// set the string in the word text
            logger.info(s"$temp found with the pattern ${patternFound.get}")
            output.collect(word, one)// collect the output
          }
        }
      }
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    logger.info("Reducer  for the Job-1 Starting")
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum1 = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get())) // Add the values together to form a single key value pair for each log type
      output.collect(key, new IntWritable(sum1.get()))

  // JOB - 2 - To find the number of occurrences with ERROR log message with injected string Pattern.
  // The following mapreduce pair is used for performing the job 2 which identifies number of ERROR messages with the injected pattern in the given time interval

  class Map1 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    logger.info("Mapper  for the Job-2 Starting")
    private final val one = new IntWritable(1)
    private val word = new Text()
    private val configMR1 = ConfigFactory.load() //loading up the configuration from application.conf
    private val timeRegex = configMR1.getString("commonMR.timeRegex").r // The time pattern is set using the regex
    private val errorRegex = configMR1.getString("MR1.errorRegex").r// The error regex will help us identify if the pattern exists in a given log message
    private var startTime = LocalTime.parse(configMR1.getString("MR1.startTime"))// var is used instead of val in order to save constantly changing start and end time
    private var endTime = LocalTime.parse(configMR1.getString("MR1.endTime"))
    private var startTimeString = startTime.toString()
    private var endTimeString = endTime.toString()
    private val intervalDuration = configMR1.getInt("MR1.intervalDuration")

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      line.split("\n").foreach { token =>
        val errorPattern = errorRegex.findFirstIn(token)// find if the error message is present
        val timeFound = timeRegex.findFirstIn(token)// find the time in the given log message
        val timeText = timeFound.get
        val timeLT = LocalTime.parse(timeText)
        if(errorPattern!=None){
          // Check if the time is bound between the interval mentioned. That is 20 minutes
          if (timeLT.isAfter(endTime)) {
            startTime = endTime.plusSeconds(1)
            endTime = endTime.plusMinutes(intervalDuration)
            startTimeString = startTime.toString()
            endTimeString = endTime.toString()
          }
          //set the string to define each interval from start time to end time
            val outputInterval = startTimeString + " to " + endTimeString
            word.set(outputInterval)
            output.collect(word, one)
          }
        }

  class Reduce1 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    logger.info("Mapper  for the Job-2 Starting")
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum2 = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get())) //Similar to Map from the previous one add all the occurrences with the same to get a single key value pair for each log message
      output.collect(key, new IntWritable(sum2.get()))

  // JOB - 3.
  // The following map reduce will find the total number of each log message occurrence
  class Map2 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    final val one = new IntWritable(1)
    private val word = new Text()
    // setup the configuration parameters from the configuration file
    private val configMR2 = ConfigFactory.load()
    private val logRegex = configMR2.getString("commonMR.logRegex").r
    logger.info("Mapper  for the Job-3 Starting")

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      line.split("\n").foreach { token =>
        val logFound = logRegex.findFirstIn(token) // Find the log message type in the given message
        val temp = logFound.get
        word.set(temp)
        output.collect(word, one) // Collect the Log message type in an output collector
      }

  class Reduce2 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    logger.info("Mapper  for the Job-3 Starting")
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum3 = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get())) // Similar to the
      output.collect(key, new IntWritable(sum3.get()))

  // JOB - 4
  // To find the maximum string length that contains the pattern for each log message type

  class Map3 extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    logger.info("Mapper  for the Job-4 Starting")
    private val word = new Text()
    private val configMR3 = ConfigFactory.load() // Load up the configuration file
    private val logRegex = configMR3.getString("commonMR.logRegex").r // Log Regex to identify the log message
    private val patternRegex = configMR3.getString("commonMR.patternRegex").r // Pattern Regex to identify the injected pattern

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      line.split("\n").foreach { token =>
        val logFound = logRegex.findFirstIn(token)
        val patternFound = patternRegex.findFirstIn(token)
        if (patternFound != None) {
          val temp = logFound.get
          val lengthPattern = token.split(" ").last.length // The last gives us the last part of the string that is separated by the given separator
          val length = IntWritable(0)
          length.set(lengthPattern)
          word.set(temp)
          output.collect(word,length) //Output is collected with log message as key, and length as value
        }
      }
  class Reduce3 extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    logger.info("Mapper  for the Job-4 Starting")
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val maximum = values.asScala.foldLeft(0)(_ max _.get) // Fold left will reduce multiple values into a single value based on a given condition
      output.collect(key, new IntWritable(maximum))

  // The second part of the Job-4 to perform sorting is carried out by the following map and reducer. The mapper takes in the output of the  Job-2
  class Map4 extends MapReduceBase with Mapper[LongWritable,Text,IntWritable, Text] :
    logger.info("Mapper  for the Job-2-part 2  Starting")
    private val timeFind = "\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[ ]to[ ]\\d{2}:\\d{2}:\\d{2}\\.\\d{3}".r //regex used to find the time from the log message
    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable,Text], reporter: Reporter): Unit =
      val temp = value.toString
      val tempNumber = temp.split(",").last.toInt //get the occurrence value from the string
      val resultNumber = new IntWritable(-tempNumber) // convert the value into a negative value
      val tempTime = timeFind.findFirstIn(temp).get
      val tempTimetext = new Text(tempTime) //
      println(tempTimetext)
      output.collect(resultNumber,tempTimetext)
  class Reduce4 extends MapReduceBase with Reducer[IntWritable,Text,Text, IntWritable] :
    override def reduce(key: IntWritable, values: java.util.Iterator[Text], output: OutputCollector[Text,IntWritable], reporter: Reporter): Unit =
      while(values.hasNext){
        val textTemp = values.next()
        val IntTemp = key.get()
        output.collect(textTemp,new IntWritable(-IntTemp))
      }

  @main private def runMapReduce(inputPath: String, outputPath: String)=
    // Initialize the job configuration for Job-1
    // The following job will find the frequency with which the string pattern occurs for a given log message.
    // JOB-1
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: JobConf = new JobConf(this.getClass)
    val outputPath1 = outputPath.concat("/job1")
    conf.setJobName("Job - 1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map]) // Initialize Mapper , Reducer, and combiner class
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.set("mapreduce.output.textoutputformat.separator", ",")    // Define the output files format.
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath1))
    logger.info("Mapper and Reducer working for JOB-1")
    JobClient.runJob(conf)
    // The following job will find the frequency with which the error log message occurs with pre-defined intervals.
    // JOB - 2
    val conf1: JobConf = new JobConf(this.getClass)     // Initialize the job configuration for Job-2
    conf1.setJobName("Job - 2")
    val outputPath2 = outputPath.concat("/job2")
    conf1.set("mapreduce.job.maps", "5")
    conf1.set("mapreduce.job.reduces", "1")
    conf1.setOutputKeyClass(classOf[Text])
    conf1.setOutputValueClass(classOf[IntWritable])
    conf1.setMapperClass(classOf[Map1])    //Initialize the mapper, reducer and combiner class for the job
    conf1.setCombinerClass(classOf[Reduce1])
    conf1.setReducerClass(classOf[Reduce1])
    conf1.set("mapreduce.output.textoutputformat.separator", ",") //Define the output files format.
    conf1.setInputFormat(classOf[TextInputFormat])
    conf1.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf1, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf1, new Path(outputPath2))
    JobClient.runJob(conf1)
    // The following job will find the total number of occrruences of each log message.
    // JOB - 3
    val conf2: JobConf = new JobConf(this.getClass) // Initialize the job configuration for Job-3
    conf2.setJobName("Job - 3")
    val outputPath3 = outputPath.concat("/job3")
    conf2.set("mapreduce.job.maps", "1")
    conf2.set("mapreduce.job.reduces", "1")
    conf2.setOutputKeyClass(classOf[Text])
    conf2.setOutputValueClass(classOf[IntWritable])
    conf2.setMapperClass(classOf[Map2]) // Initialize Mapper , Reducer, and Combiner class
    conf2.setCombinerClass(classOf[Reduce2])
    conf2.setReducerClass(classOf[Reduce2])
    conf2.set("mapreduce.output.textoutputformat.separator", ",") //Define the output files format.
    conf2.setInputFormat(classOf[TextInputFormat])
    conf2.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf2, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf2, new Path(outputPath3))
    JobClient.runJob(conf2)
    //  The following job will count the largest string length with injected string pattern
    //   JOB - 4
    val conf3: JobConf = new JobConf(this.getClass) // Initialize the job configuration for Job-4
    conf3.setJobName("Job - 4")
    val outputPath4 = outputPath.concat("/job4")
    conf3.set("mapreduce.job.maps", "1")
    conf3.set("mapreduce.job.reduces", "1")
    conf3.setOutputKeyClass(classOf[Text])
    conf3.setOutputValueClass(classOf[IntWritable]) // Initialize Mapper , Reducer, and combiner class
    conf3.setMapperClass(classOf[Map3])
    conf3.setCombinerClass(classOf[Reduce3])
    conf3.setReducerClass(classOf[Reduce3])
    conf3.setInputFormat(classOf[TextInputFormat]) //Define the output files format.
    conf3.set("mapreduce.output.textoutputformat.separator", ",")
    conf3.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf3, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf3, new Path(outputPath4))
    JobClient.runJob(conf3)
    //    Part -2 of the Job2.
    //    The following job configuration will be performing the sorting of the previously obtained data
    val conf4: JobConf = new JobConf(this.getClass)
    conf4.setJobName("Job - 4")
    val outputPath5 = outputPath.concat("/job2Part2")
    val config2 = ConfigFactory.load()
    val inputPath2 = config2.getString("MR1.inputPath") // Initialize the job configuration for Job-5-second half of job-2
    conf4.set("mapreduce.job.maps", "1")
    conf4.set("mapreduce.job.reduces", "1")
    conf4.setOutputKeyClass(classOf[IntWritable])
    conf4.setOutputValueClass(classOf[Text]) // Initialize Mapper and Reducer class
    conf4.setMapperClass(classOf[Map4])
    conf4.setReducerClass(classOf[Reduce4]) //Define the output files format.
    conf4.set("mapreduce.output.textoutputformat.separator", ",")
    conf4.setOutputFormat(classOf[TextOutputFormat[Text,IntWritable]])
    FileInputFormat.setInputPaths(conf4, new Path(inputPath2))
    FileOutputFormat.setOutputPath(conf4, new Path(outputPath5))
    JobClient.runJob(conf4)
