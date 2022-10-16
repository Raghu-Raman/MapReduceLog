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

