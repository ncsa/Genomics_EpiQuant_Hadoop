git pull
mvn clean
mvn compile
mvn package
hadoop fs -rm -r *.Split*
hadoop jar target/SEMS-Hadoop-0.1.0.jar SEMSHadoop /user/rchui2/data/ /user/rchui2/pheno/pheno.txt
hadoop fs -cat output/part-r-00000