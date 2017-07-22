git pull
mvn clean
mvn compile
mvn package
hadoop fs -rm -r *.Split*
hadoop fs -rm -r DataBuilder
hadoop fs -rm -r DataCleaner
hadoop fs -rm -r .staging/*
hadoop fs -rm -r .Trash/*
export HADOOP_CLASSPATH=/ui/ncsa/rchui2/SEMS-Hadoop/target
echo $HADOOP_CLASSPATH
hadoop jar target/SEMS-Hadoop-0.1.0.jar sems.SEMSHadoop /user/rchui2/data/snps.txt /user/rchui2/pheno/pheno.txt
# hadoop fs -cat Phenotype-*.Split-*/part-r-00000