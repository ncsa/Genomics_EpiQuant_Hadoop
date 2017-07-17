package sems;

import managers.JobManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class SEMSHadoop {
    public static void main(String[] args) throws Exception {
        getPhenotypes(args);
        JobManager jobManager = new JobManager();
        jobManager.run(args);
        System.out.println("Hello World");
        System.exit(0);
    }

    public static void getPhenotypes(String[] args) throws IOException {
        Path path = new Path("hdfs:" + args[2]);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        boolean first = true;
        while((line = buff.readLine()) != null) {
            if (!first) {
                System.out.println(line);
            } else {
                first = false;
            }
        }
        System.exit(0);
    }
}