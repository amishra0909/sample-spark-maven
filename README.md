This is a sample project to build and package scala based spark app in Maven.

Build: mvn package

Run: spark-submit --class com.abhishek.MnMCount target/scala-spark-app-1.0-SNAPSHOT.jar data/mnm_dataset.csv
