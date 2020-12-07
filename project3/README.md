# Problem 2 SparkSQL
Use the Transaction dataset T and Customer dataset C that you created in Project 1 and create a Spark workflow to answer the following query. Report the number of male and the number of female customers in the country Z, where country Z is the country that have the largest sum of TransTotal for all its customers.

# Problem 4 SparkSQL (Matrix-Matrix Multiplication)
Matrix-Matrix multiplication is a practical problem that is needed in many applications. It can be done in a distributed highly scalable way. Under project 3 directory, you will find some slides on how to design map-reduce jobs to implement a distributed algorithm. Also, for more details, see Section 2.3.9 in Book “mining of massive dataset” (in Canvas under “Files/pdf Books”).

In this question, you are asked to create two files M1 & M2, where M1 represents Matrix 1, and M2 represents Matrix 2. The structure of each file is as illustrated in the posted slides (3 columns in each file). Assume the dimensions of M1 and M2 are 500x500. Populate the files with random integer values for each entry.
Then use Spark SQL (data frames) to multiply M1 and M2. That is, implement the map-reduce jobs (joins & aggregations) in terms of Spark SQL operators on data frames.

The output should be another file that goes back to HDFS storing the result matrix in the same format as the inputs (i j, value)
