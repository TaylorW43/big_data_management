# Problem 2 (K-Means Clustering) 
K-Means clustering is a popular algorithm for clustering similar objects into K groups (clusters). It starts with an initial seed of K points (randomly chosen) as centers, and then the algorithm iteratively tries to enhance these centers. The algorithm terminates either when two consecutive iterations generate the same K centers, i.e., the centers did not change, or a maximum number of iterations is reached.

Hint: You may reference these links to get some ideas (in addition to the course slides):
* http://en.wikipedia.org/wiki/K-means_clustering#Standard_algorithm
* https://cwiki.apache.org/confluence/display/MAHOUT/K-Means+Clustering

## Map-Reduce Job
Write map-reduce job(s) that implement the K-Means clustering algorithm as given in the course slides. The algorithm should terminates if either of these two conditions become true:
* The K centers did not change over two consecutive iterations
* The maximum number of iterations (make it six (6) iterations) has reached.
* Apply the tricks given in class and in the 2nd link above such as: use of a combiner, use a single reducer

## Dataset

## Input Parameters
The Java program should accept the HDFS file location containing the initial K centroids as a parameter. This is the file, which will be broadcasted to all mappers in the 1st round. K can be any value within the range of [10...100]
