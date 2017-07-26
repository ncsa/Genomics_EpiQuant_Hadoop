yarn application -list | awk '$5=="rchui2"{print $1}' | xargs -L 1 -n 1 -P 8 yarn application -kill 

# Gets the newest code.
git pull

# Clean, compile, and package code.
mvn clean
mvn compile
mvn package

# Set workner node access permissions.
chmod -R 755 ./

# Remove older runs.
hadoop fs -rm -r DataBuilder
hadoop fs -rm -r DataCleaner
hadoop fs -rm -r .staging/*
hadoop fs -rm -r .Trash/*
hadoop fs -rm -r *.Split*

# Set global HADOOP_CLASSPATH for jar access.
export HADOOP_CLASSPATH=/ui/ncsa/rchui2/SEMS-Hadoop/target/SEMS-Hadoop-0.1.0.jar
echo $HADOOP_CLASSPATH

# Run map reduce job.
hadoop jar target/SEMS-Hadoop-0.1.0.jar sems.SEMSHadoop /user/rchui2/data/snps.txt /user/rchui2/pheno/pheno.4.test.txt