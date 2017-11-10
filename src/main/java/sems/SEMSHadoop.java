package sems;

import managers.DataBuilder;
import managers.DataCleaner;
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

    /**
     * The driver for the step-wise model selection.
     * Tracks jobs as they are running and reports on program status.
     * Checks jobs for completion, adds new jobs, and removes old jobs.
     * Params:
     *     args (String[]) user passed parameters.
     */
    public static void main(String[] args) throws Exception {
        // Start timer.
        long start = System.nanoTime();
        String message, baseDir, prevDir;

        // Compute all possible pair-wise elements.
        message = " [Task = Adding DataBuilder]";
        runningTime(start, 1, false, message);
        DataBuilder dataBuilder = new DataBuilder();
        dataBuilder.run(args[1], "DataBuilder");
        message = " [Task = Removing DataBuilder]";
        runningTime(start, 1, false, message);

        // Remove pair-wise duplicates.
        message = " [Task = Adding DataCleaner]";
        runningTime(start, 1, false, message);
        DataCleaner dataCleaner = new DataCleaner();
        dataCleaner.run("/user/rchui2/DataBuilder", "DataCleaner");
        message = " [Task = Removing DataCleaner]";
        runningTime(start, 1, false, message);

        // Get the phenotypes to iterate over.
        ArrayList<String> phenoList = getPhenotypes(args);
        ArrayList<Job> jobList = new ArrayList<Job>();
        ArrayList<int[]> splits = new ArrayList<int[]>();

        // Submit initial set of jbos to the job list.
        JobManager jobManager = new JobManager();
        for (int i = 0; i < phenoList.size(); i++) {
            splits.add(new int[2]);
            splits.get(i)[0] = i; // Phenotype number
            splits.get(i)[1] = 1; // Split number

            message = " [Task = Adding P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
            runningTime(start, jobList.size(), false, message);

            baseDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] + "/";
            jobList.add(jobManager.run("/user/rchui2/DataCleaner/part-r-00000", phenoList.get(i), ".", baseDir, splits.get(i)[0], splits.get(i)[1]));
        }

        boolean running = true;

        // Track running jobs until none are left.
        while (running) {
            runningTime(start, jobList.size(), false, "");
            int size = jobList.size();

            // Check each job if they are completed. If so then remove.
            for (int i = 0; i < size; i++) {
                if (jobList.get(i).isComplete()) {
                    message = " [Task = Removing P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
                    runningTime(start, jobList.size(), false, message);
                    jobList.remove(i);

                    prevDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] +  "/";
                    splits.get(i)[1]++;
                    baseDir = "/user/rchui2/Phenotype-" + splits.get(i)[0] + ".Split-" + splits.get(i)[1] +  "/";

                    // Check that iterative process is completed.
                    if (!isDone(prevDir)) {
                        message = " [Task = Adding P-" + splits.get(i)[0] + ".S-" + splits.get(i)[1] + "]";
                        runningTime(start, jobList.size(), false, message);
                        jobList.add(jobManager.run("/user/rchui2/DataCleaner/part-r-00000", phenoList.get(i), getModel(prevDir), baseDir, splits.get(i)[0], splits.get(i)[1]));
                        phenoList.add(phenoList.remove(i));
                        splits.add(splits.remove(i));
                    } else { // Done, remove job information.
                        phenoList.remove(i);
                        splits.remove(i);
                    }

                    i--;
                    size--;
                }
            }

            // Check for program completion.
            if (jobList.isEmpty()) {
                running = false;
            }
            TimeUnit.SECONDS.sleep(10);
        }
        runningTime(start, jobList.size(), true, "");
        System.exit(0);
    }

    /**
     * Displays tracking information and updates to map reduce processes.
     * Params:
     *     start (long) start time of the program.
     *     size (int) number of jobs left.
     *     finished (boolean) program is finished.
     *     message (String) additional message to print with tracking information.
     */
    public static void runningTime(long start, int size, boolean finished, String message) {
        long current, rawSeconds, nSeconds, nMinutes, hours;
        String seconds, minutes;
        
        // Compute relative time values.
        current = System.nanoTime();
        rawSeconds = (current - start) / 1000000000;
        nSeconds = ((current - start) / 1000000000) % 60;
        nMinutes = (rawSeconds / 60) % 60;
        hours = rawSeconds / 60 / 60;

        // Compute pretty time values.
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

    /**
     * Checks a phenotype's model has been fully computed.
     * Returns true if done.
     * Params:
     *     baseDir (String) the base directory that contains the job output.
     */
    public static boolean isDone(String baseDir) throws IOException{
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
    }

    /**
     * Parses the model file to be fed back into the next iteration.
     * Returns the model as a string. Each model is internally delimited by commas and delimited
     * from each other by newline characters.
     * Params:
     *     baseDir (String) the base directory that contains the job output.
     */
    public static String getModel(String baseDir) throws IOException {
        Path path = new Path("hdfs:" + baseDir + "model-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
        String model = buff.readLine();
        String line;
        while ((line = buff.readLine()) != null) {
            model += "\n" + line;
        }
        if ("".equals(model) || model == null || model.isEmpty()) {
            model = ".";
        }
        return model;
    }

    /**
     * Gets the phenotype (y values) from the specified phenotype file.
     * Returns a list of the phenotypes to run jobs over.
     * Params:
     *     args (String[]) user passed program parameters.
     */
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
        return null; // To satisfy return condition. Will never reach.
    }
}
