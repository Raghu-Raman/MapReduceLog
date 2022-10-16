# MapReduceLog
This is a map reduce project that takes in log message as input and produces pre-defined statistics as output.

## Aim
The objective of this project is to deploy a distributed program that will produce statistics obtained from processing of log messages that will be generated.
The project will be written in scala and will be deployed in Amazon Web Services (AWS) Elastic Map Reduce [AWS EMR](https://us-east-2.console.aws.amazon.com/elasticmapreduce/home?region=us-east-2#)

## MapReduce
MapReduce enables parallel processing of data. The distributed computing model enables to process data at a faster rate when compared to pipelined processing. The data sent into the data,usually big data, is split into smaller sections called as chunks or shards. The model consists of two parts. They are
  - Mappers 
  - Reducers
#### Mappers
The mappers are the first stage of the architechture. The mappers take in a set of key value parts and process them to form a map of key value pairs. The output of the mapper will be pipelined to the reducer. The key value pairs produced by mappers are intermediate key value pairs. If more than one mapper is pipelined to a single reducer, a combiner will come into play.
![MapReduce-Architecture](https://user-images.githubusercontent.com/78893470/196014902-508d2420-2e0d-49a9-95b4-723d2c4d0ba8.jpg)

#### Reducers
The reducer is the second stage of the architecture. The reducers receive the input from the mappers/the combiners. The reducer,as the name suggests, will reduce the multiple key value pairs with a single key with multiple values linked to it. The method of reduction depends on the algorithm provided to the reducer.

## Jobs
#### Job 1 
The job-1 deals with processing the input file and calculate the number of occurrences of injected string pattern for a given log message type. The log message types are 
  - INFO
  - WARN
  - DEBUG
  - ERROR
#### Job 2
The job-2 deals with obtaining the number of occurrences of 'ERROR' log message with injected string pattern for a given time interval,and the time intervals has to be sorted in descending order.
#### Job 3
The job-3 deals with to calculate the total number of occurrences for each log message type.
#### Job 4
The job-4 deals with obtaining the length of the longest string for a given log message with injected string pattern.
## Implementaion
Create the main class with all the job configurations with. Each job configuration is given with a specific task. The task will define the behaviour of the mappers and reducers in the job. The ``` application.conf ``` will contain all the input parameters and all the necessary input configurations for all the jobs. The ``` logback.xml``` file will help us logging the details of the programs. 
#### Mapper class - Reducer class
This will give us the distribution of the injected patterns for each log message.

#### Mapper1 class - Reducer1 class
This will give us the distribution of the injected patterns for ``` ERROR``` log message for a pre-defined time interval. This will be part 1 of the job-2

#### Mapper4 class - Reducer4 class
This will give us sorting of the output from the ``` Reducer1``` class. Thus completing the second part of the job-2.

#### Mapper2 class - Reducer2 class
The mapper will process the input to identify the messages with injected string pattern and add 1 as a value for it. The reducer will give the total sum of all the injected string pattern for all log messages.

#### Mapper3 class - Reducer3 class
The mapper will count the length of all the string pattern for each log message and create set of key value pairs. The reducer will reduce the values to obtain the maximum length.
Z
