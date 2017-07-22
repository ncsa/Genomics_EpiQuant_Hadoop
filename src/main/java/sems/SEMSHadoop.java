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
        System.out.println(countLines("/user/rchui2/data/snps.txt"));
        System.exit(0);
        ArrayList<String> phenoList = getPhenotypes(args);
        ArrayList<Job> jobList = new ArrayList<Job>();
        ArrayList<int[]> splits = new ArrayList<int[]>();
        long start = System.nanoTime();
        String message, baseDir, prevDir;

        // Submit jobs by to the job list.
        JobManager jobManager = new JobManager();
        for (int i = 0; i < phenoList.size(); i++) {
            splits.add(new int[2]);
            splits.get(i)[0] = i; // Phenotype number
            splits.get(i)[1] = 1; // Split number

            message = " [Task = Adding P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
            runningTime(start, jobList.size(), false, message);

            baseDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] + "/";
            jobList.add(jobManager.run(args[1], phenoList.get(i), ".", baseDir, splits.get(i)[0], splits.get(i)[1]));
        }

        boolean running = true;

        // Track running jobs until none are left.
        while (running) {
            runningTime(start, jobList.size(), false, "");
            int size = jobList.size();

            // Remove jobs if completed.
            for (int i = 0; i < size; i++) {
                if (jobList.get(i).isComplete()) {
                    message = " [Task = Removing P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
                    runningTime(start, jobList.size(), false, message);
                    jobList.remove(i);

                    prevDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] +  "/";
                    splits.get(i)[1]++;
                    baseDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] +  "/";
                    if (!isDone(prevDir)) {
                        message = " [Task = Adding P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
                        runningTime(start, jobList.size(), false, message);
                        jobList.add(jobManager.run(args[1], phenoList.get(i), getModel(prevDir), baseDir, splits.get(i)[0], splits.get(i)[1]));
                        phenoList.add(phenoList.remove(i));
                        splits.add(splits.remove(i));
                    } else {
                        phenoList.remove(i);
                        splits.remove(i);
                    }

                    i--;
                    size--;
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

    // Displays tracking information and process updates.
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
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Finishing..] [Jobs = " + size + "]" + message);
        } else {
            System.out.println("[" + hours + "h:" + minutes + "m:" + seconds + "s] [Status = Running....] [Jobs = " + size + "]" + message);
        }
    }

    public static boolean isDone(String baseDir) throws IOException{
        Path path = new Path("hdfs:" + baseDir + "significance-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = buff.readLine().trim();
            buff.close();
            fs.close();
            if (Double.parseDouble(line) < 0.05) {
                return false;
            }
        }
        return true;
    }

    public static String getModel(String baseDir) throws IOException {
        Path path = new Path("hdfs:" + baseDir + "model-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
        String model = buff.readLine();
        String line;
        while ((line = buff.readLine()) != null) {
            model += "\n" + line;
        }
        return model;
    }

    // Gets phenotypes (y values) from the specified phenotype file.
    public static ArrayList<String> getPhenotypes(String[] args) throws IOException {
        try {
            Path path = new Path("hdfs:" + args[2]);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            ArrayList<String> phenoList = new ArrayList<String>();
            while((line = buff.readLine()) != null) {
                phenoList.add(line);
            }
            buff.close();
            fs.close();
            return phenoList;
        } catch (Exception e) {
            System.err.println("Could not parse a phenotype file.");
            System.exit(1);
        }
        return null;
    }

    public static long countLines(String filePath) throws IOException {
        Path path = new Path("hdfs:" + filePath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
        long count = 0;
        while (buff.readLine() != null) {
            count++;
        }
        return count;
    }
}
