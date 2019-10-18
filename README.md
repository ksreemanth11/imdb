If given more time:
1. Would have written unit test cases.
2. Would have used Datasets with case classes.
3. Would have converted all the csv files into parquet as all of them are structured data.
4. Would have implemented dynamic partitioning logic

Assumptions:
1. Selected only "movie" and "tvMovie" titles for Q-1


How to run in local mode:

Step:1 Create a new folder for input sources under Documents folder(assuming using Mac laptop)
Step:2 Downlad all the files from https://datasets.imdbws.com/. 
Step:3 Clone project from https://github.com/ksreemanth11/imdb.git
Step:4 Import the project into intellij Idea IDE.
Step:5 Auto import all the dependencies.
Step:6 Edit src/main/resources/application.conf
		change root_dir = "/Users/nanivani/Documents/imdb-data" to the path created in Step:1
Step:7 Build the project
Step:8 For the solution to Q-1 Goto to src/main/scala/com/imdb/TopMovies.scala and right-click and RUN to execute.
Step:9 For the solution to Q-2 Goto to src/main/scala/com/imdb/TopMovieWatchers.scala and right-click and RUN to execute


How to run in Cluster mode:

Step:1 From src/main/scala/com/imdb/TopMovies.scala & src/main/scala/com/imdb/TopMovieWatchers.scala
edit "SparkSession.builder().master("local").appName("IMDB Case Study").getOrCreate()" to remove ".master("local")" 

Step:2 Build and create a jar with sbt-assembly

Step:3 Copy the jar to cluster

Step:4 For solution to Q1 run

"spark-submit —class com.imdb.TopMovies —master yarn —deploy-mode cluster —num-executors 6 —executor-core 5 —executor-memory 15g —drivers-memory 10g imdb-usecase-assembly-0.1.jar"

Step:5 For solution to Q2 run

"spark-submit —class com.imdb.TopMovieWatchers —master yarn —deploy-mode cluster —num-executors 6 —executor-core 5 —executor-memory 15g —drivers-memory 10g imdb-usecase-assembly-0.1.jar"

