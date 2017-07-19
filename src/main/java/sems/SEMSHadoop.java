package sems;

import managers.JobManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class SEMSHadoop {
    public static void main(String[] args) throws Exception {
        ArrayList<String> phenoList = getPhenotypes(args);
        ArrayList<Job> jobList = new ArrayList<Job>();
        ArrayList<int[]> splits = new ArrayList<int[]>();
        long start = System.nanoTime();

        // Submit jobs by to the job list.
        JobManager jobManager = new JobManager();
        for (int i = 0; i < phenoList.size(); i++) {
            splits.add(new int[2]);
            splits.get(i)[0] = i; // Phenotype number
            splits.get(i)[1] = 1; // Split number
            runningTime(start, jobList.size(), false, " [Task = Adding P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]");
            jobList.add(jobManager.run(args, phenoList.get(i), splits.get(i)[0], splits.get(i)[1]));
        }

        boolean running = true;

        // Track running jobs until none are left.
        while (running) {
            runningTime(start, jobList.size(), false, "");
            // Remove jobs if completed.
            for (int i = 0; i < jobList.size(); i++) {
                if (jobList.get(i).isComplete()) {
                    runningTime(start, jobList.size(), false, " [Task = Removing P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]");
                    jobList.remove(i);
                    splits.remove(i);
                    phenoList.remove(i);
                }
            }
            if (jobList.isEmpty()) {
                running = false;
            }
            TimeUnit.SECONDS.sleep(2);
        }
        runningTime(start, jobList.size(), true, "");
        System.exit(0);
    }

    public static void runningTime(long start, int size, boolean finished, String message) {
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
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Finish... ] [Jobs = " + size + "]" + message);
        } else {
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Running...] [Jobs = " + size + "]" + message);
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
