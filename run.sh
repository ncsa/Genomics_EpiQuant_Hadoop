git pull
mvn clean
mvn compile
mvn package
hadoop fs -rm -r *.Split*
hadoop fs -rm -r DataBuilder
hadoop jar target/SEMS-Hadoop-0.1.0.jar SEMSHadoop /user/rchui2/data/snps.txt /user/rchui2/pheno/pheno.txt
# hadoop fs -cat Phenotype-*.Split-*/part-r-00000