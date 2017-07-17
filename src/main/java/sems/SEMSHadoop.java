package sems;

import managers.JobManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class SEMSHadoop {
    public static void main(String[] args) throws Exception {
        ArrayList<String> phenoList = getPhenotypes(args);
        boolean running = true;
        long start = System.nanoTime();
        int iterations = 0;
        while (running) {
            iterations++;
            runningTime(start);
            if (iterations > 5) {
                running = false;
            }
            TimeUnit.SECONDS.sleep(10);
        }
        System.exit(0);
        JobManager jobManager = new JobManager();
        jobManager.run(args);
        System.out.println("Hello World");
        System.exit(0);
    }

    public static void runningTime(long start) {
        long current = System.nanoTime();
        long rawSeconds = (current - start) / 1000000000;
        long seconds = ((current - start) / 1000000000) % 60;
        long minutes = (rawSeconds / 60) % 60;
        long hours = rawSeconds / 60 / 60;
        System.out.println("Elapsed Time: " + hours + "h " + minutes + "m " + seconds + "s [Status: Running...]");
    }

    public static ArrayList<String> getPhenotypes(String[] args) throws IOException {
        try {
            Path path = new Path("hdfs:" + args[2]);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            boolean first = true;
            ArrayList<String> phenoList = new ArrayList<String>();
            while((line = buff.readLine()) != null) {
                if (!first) {
                    phenoList.add(line);
                } else {
                    first = false;
                }
            }
            return phenoList;
        } catch (Exception e) {
            System.err.println("Could not parse a phenotype file.");
            System.exit(1);
        }
        return null;
    }
}
