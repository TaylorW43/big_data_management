The final project code cannot be uploaded right now for personal reasons.

# Project 2: Implementing Local Outlier Detection (LOF) on Hadoop or Spark
In anomaly detection, the local outlier factor (LOF) is an algorithm for finding anomalous data points by measuring the local deviation of a given data point with respect to its neighbors. The algorithm is suitable for detecting the outliers even if the data space has many areas with different densities as illustrated in the figure.

![final_project_pic](<https://user-images.githubusercontent.com/63271980/101364625-c787f200-3870-11eb-9bea-b59663ae2d5c.png>)

LOF is a centralized algorithm, i.e., runs on a single machine. There are some attempts to make it distributed (slides available)

## Reading Material (Get Familiar with LOF)
* [LOF](http://www.dbs.ifi.lmu.de/Publikationen/Papers/LOF.pdf)
* [Distance_Based](http://www.vldb.org/conf/1998/p392.pdf)

## Your Project
* Build a good understanding on the LOF algorithm and how it works in a centralized environment
* Build a good understanding on another outlier detection algorithm such that you can understand the pros and cons of LOF (a suggested one is Distance-based Outlier Detection- A reference is
provided above)
* Come up with a design for making LOF a distributed algorithm that can run on Hadoop
* You need to think of any optimizations you can do to speed up the execution.
* You will learn many optimization tricks from the Team presentations in class. Think which of these
optimizations apply to your project and you can do something similar.
