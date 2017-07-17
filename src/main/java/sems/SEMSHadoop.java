package sems;

import managers.JobManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class SEMSHadoop {
    public static void main(String[] args) throws Exception {
        ArrayList<String> phenoList = getPhenotypes(args);
        ArrayList<Job> jobList = new ArrayList<Job>();
        JobManager jobManager = new JobManager();
        for (int i = 0; i < phenoList.size(); i++) {
            System.out.println("Adding Job: " + i);
            jobList.add(jobManager.run(args));
        }
        boolean running = true;
        long start = System.nanoTime();
        while (running) {
            runningTime(start, jobList.size(), false);
            for (int i = 0; i < jobList.size(); i++) {
                if (jobList.get(i).isComplete()) {
                    System.out.println("Removing Job: " + i);
                    jobList.remove(i);
                }
            }
            if (jobList.isEmpty()) {
                running = false;
            }
            TimeUnit.SECONDS.sleep(2);
        }
        runningTime(start, jobList.size(), true);
        System.exit(0);
    }

    public static void runningTime(long start, int size, boolean finished) {
        long current, rawSeconds, nSeconds, nMinutes, hours;
        String seconds, minutes;
        
        current = System.nanoTime();
        rawSeconds = (current - start) / 1000000000;
        nSeconds = ((current - start) / 1000000000) % 60;
        nMinutes = (rawSeconds / 60) % 60;
        hours = rawSeconds / 60 / 60;

        if (nSeconds < 10) {
            seconds = "0" + nSeconds;
        } else {
            seconds = String.valueOf(nSeconds);
        }
        if (nMinutes < 10) {
            minutes = "0" + nMinutes;
        } else {
            minutes = String.valueOf(nMinutes);
        }
        if (finished) {
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Finishing...] [Jobs = " + size + "]");
        } else {
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Running...] [Jobs = " + size + "]");
        }
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
