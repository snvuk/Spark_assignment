## Big Data ##

Big data refers to extremely large and complex data sets that are beyond the ability of traditional data processing software and hardware to manage and analyze effectively.

## HDFS ##

HDFS (Hadoop Distributed File System) is a distributed file system that is designed to store and manage large data sets across multiple servers in a Hadoop cluster. It is a key component of the Apache Hadoop software framework for distributed storage and processing of big data.

## Yarn ##

YARN (Yet Another Resource Negotiator) is a distributed resource management system that is a key component of the Apache Hadoop ecosystem.

## Map Reduce ##

MapReduce is a programming model and processing framework for large-scale distributed data processing, which was popularized by Apache Hadoop. It is designed to process and analyze large amounts of data in a parallel and distributed manner across a large cluster of computers.

## Hive ##

Hive is an open-source data warehousing and SQL-like query language that is built on top of Hadoop Distributed File System (HDFS) and Apache Hadoop. 

# Spark

## Spark Introduction ##

Spark is an open-source distributed computing system designed to process large datasets across a cluster of computers. It was developed at the University of California, Berkeley's AMPLab and is now maintained by the Apache Software Foundation.

## Spark Features ##

- Fast Processing
- Flexibility
- Fault tolerance
- In-memory computing
- Real-time processing
- Better analytics

## Spark Architecture ##

- Spark Drive
- Cluster Manager
- Executors
- Spark Context
- Resilient Distributed Datasets (RDDs)

## Cluster and Configuration ##

 a cluster is a set of computers that work together to process data in parallel. The cluster is managed by a cluster manager, which coordinates the allocation of resources and scheduling of tasks across the nodes in the cluster
 
 ## RDD ##
 
 RDD stands for Resilient Distributed Dataset, which is a fundamental data structure in Spark. An RDD is an immutable, fault-tolerant collection of elements that can be processed in parallel across multiple nodes in a distributed computing environment.
 
 ## Data Frame ##
 
 A DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database. DataFrames are a higher-level abstraction built on top of RDDs (Resilient Distributed Datasets), which provide a more structured and optimized view of data.
 
 ## What is Dataset ##
 
 A Dataset is a distributed collection of data that provides the benefits of both RDDs and DataFrames in Spark. Like RDDs, Datasets are strongly typed and provide compile-time type safety, while also supporting rich functions and transformations. Like DataFrames, Datasets are optimized for efficient processing and provide a more structured and efficient view of data.



