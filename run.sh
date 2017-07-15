git pull
mvn compile
mvn package
hadoop fs -rm -r output
hadoop jar target/SEMS-Hadoop-0.1.0.jar SEMSHadoop /user/rchui2/data/