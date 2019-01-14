# FastParquet Student Project (for GSOC 2019)

## Objective

Encode JSON, including the deeply nested object arrays, in parquet format using Python


## Background

Mozilla has a large Elasticsearch cluster that stores billions of JSON documents with a multitude of schemas. We believe it provides the lowest query latency per-dollar of any other document store; and it would be nice to prove it.

Unfortunately, the system we have can store and query any schema; including deeply nested object arrays. It also dynamically manages schema migration. This makes it impossible to compare other document stores and their query speeds: We can not even insert the documents into these other stores without significant manual effort, never mind compare query speeds.   

We would like to compare our current query response times to Spark on a large cluster, but naive iterate-through-raw-json-documents would not be fair to Spark. Most queries are only interested in a small number of properties; storing the documents in a columnar format would reduce the amount of data Spark must churn through to answer a query. The parquet file format is a columnar format for JSON-like documents, if we can write Parquet files, then we get the best speed out of Spark. Unfortunatly, Python has poor library support for writing deeply nested object arrays to Parquet format.  

Spark is a fixed-schema query tool, so it will not handle the schema dynamism as well as we do now, but we already have schema-management code to provide a fixed schema, therefore this is not a problem.


## Past Attempt

Lucky for us we work with open source! Last year I found `fastparquet` to be the best Parquet encoding library in Python. I attempted a patch, but I underestimated the effort and could not justify spending more time on the project. 

I broke the problem down: 

First, I had to ensure the logical encoding is correct: The `mo-parquet` project is a stand-alone project responsible for converting lists of JSON documents into Dremel-encoded columnar arrays. It also tracks the schema required to do so.

Second, I started integrating the mo-parquet code into fastparquet, but it is incomplete. The idea is to add tests for deeply nested JSON, encode them into parquet format, and recover them into Pandas dataframes. Only a couple of tests work so far.

## The plan for GSOC

I imagine a GSOC project would go something like this 

1. Install fastparquet in development so that the tests pass
2. Apply my PR to fastparquet, add some additional supporting libs
3. Get my additional test to run
3. Write a  larger test suite so we are confident in the code - **the hard part**
4. Ensure Spark can read, and query, the written files
5. Breakup the PR into a number of reasonable patches that can be submitted to the main fastparquet project

If there is time, then there is more to do

* Have our ActiveData-ETL pipeline use the new code to write Parquet files
* See if Spark can work on the parquet files at scale
