twitter-hadoop
==============

Twitter big data analysis using hadoop

System Requirements:

Any linux/unix based system
8GB of RAM recommended
2.0 Ghz+ processor
SSD prefered

Note: More RAM and a faster processor will make sure the data processing and MR jobs complete sooner.

We have not included the word count file and co occurrences output file from hadoop as they are very large in size.

Instructions to run the jar file :

To collect tweet data
$ java jar TweetCollector.jar 

To run the other programs follow the below instructions:
1. Wordcount
input folder -> /input/<list all tweet files>
output folder -> /output
$ java jar DICProject.jar sample.WordCount <input_dir> <output_dir>

2. Co-occuring word
input folder -> /input/<list all tweet files>
output folder -> /output
$ java jar DICProject.jar cooccurpair.WordCountPair <input_dir> <output_dir>
$ java jar DICProject.jar cooccurstripes.WordCountPair <input_dir> <output_dir>

3. kmeans
input folder -> /user/naren/kmeans/<followers_count_file>
output folder -> /user/naren/kmeansoutput
$ java jar DICProject.jar kmeans.KMeansCluster

4. Shortest Path
input folder -> /input/<input_graph_file>
output folder -> /output
$ java jar DICProject.jar graph.ShortestPath <input_dir> <output_dir>


