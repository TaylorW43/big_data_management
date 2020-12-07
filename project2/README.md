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
* ./kmeans/input/test_cen.csv
* ./kmeans/input/test_p.csv

## Input Parameters
The Java program should accept the HDFS file location containing the initial K centroids as a parameter. This is the file, which will be broadcasted to all mappers in the 1st round. K can be any value within the range of [10...100]

# Problem 4 (Distance-Based Outlier Detection Clustering)
Outliers are objects in the data that do not conform to the common behavior of the other objects. There are many definitions for outliers. One common definition is “distance-based outliers”. In this definition (see the figure below), you are given two parameters, radius r and threshold k, and a point p is said to be outlier iff: “Within a circle around p (p is the center) of radius r, less than k neighbors are found”. And point p is said to be inlier (Not outlier) iff: “Within a circle around p (p is the center) of radius r, more than or equal to k neighbors are found”

[![p4_pic](<https://user-images.githubusercontent.com/63271980/101361277-c3f26c00-386c-11eb-91ab-5821382b4eff.png>
)]
