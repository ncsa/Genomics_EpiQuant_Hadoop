package sems;

import managers.BackwardManager;
import managers.ForwardManager;
import utilities.Model;

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
        ArrayList<String> fPhenoList = getPhenotypes(args);
        ArrayList<Job> fJobList = new ArrayList<Job>();
        ArrayList<int[]> fSplits = new ArrayList<int[]>();

        ArrayList<String> bPhenoList = new ArrayList<String>();
        ArrayList<Job> bJobList = new ArrayList<Job>();
        ArrayList<int[]> bSplits = new ArrayList<int[]>();
        long start = System.nanoTime();
        String message;

        // Submit jobs by to the job list.
        ForwardManager fManager = new ForwardManager();
        BackwardManager bManager = new BackwardManager();
        for (int i = 0; i < fPhenoList.size(); i++) {
            fSplits.add(new int[2]);
            fSplits.get(i)[0] = i; // Phenotype number
            fSplits.get(i)[1] = 1; // Split number
            message = " [Task = Adding F.P-" + fSplits.get(i)[0] + ".S-" + fSplits.get(i)[1] + "]";
            runningTime(start, fJobList.size(), false, message);
            fJobList.add(fManager.run(args, fPhenoList.get(i), fSplits.get(i)[0], fSplits.get(i)[1]));
        }

        boolean running = true;

        // Track running jobs until none are left.
        while (running) {
            runningTime(start, fJobList.size(), false, "");
            int size = fJobList.size();
            // Remove jobs if completed.
            for (int i = 0; i < fJobList.size(); i++) {
                if (fJobList.get(i).isComplete()) {
                    message = " [Task = Removing F.P-" + fSplits.get(i)[0] + ".S-" + fSplits.get(i)[1] + "]";
                    runningTime(start, fJobList.size() + bJobList.size(), false, message);
                    fJobList.remove(i);

                    if (true) {
                        String baseDir = "/user/rchui2/Phenotype-" + fSplits.get(i)[0] + ".Split-" + fSplits.get(i)[1] + "/";
                        message = " [Task = Adding B.P-" + fSplits.get(i)[0] + ".S-" + fSplits.get(i)[1] + "]";
                        runningTime(start, fJobList.size(), false, message);
                    //     bJobList.add(bManager.run(baseDir + "part-r-00000", fPhenoList.get(i), baseDir));
                    }
                    fSplits.remove(i);
                    fPhenoList.remove(i);
                    i--;
                    size--;
                }
            }

            size = bJobList.size();
            for (int i = 0; i < bJobList.size(); i++) {
                if (bJobList.get(i).isComplete()) {
                    message = " [Task = Removing B.P-" + bSplits.get(i)[0] + ".S-" + bSplits.get(i)[1] + "]";
                    runningTime(start, bJobList.size() + bJobList.size(), false, message);
                    bJobList.remove(i);
                    i--;
                    size--;
                }
            }
            if (fJobList.isEmpty() && bJobList.isEmpty()) {
                running = false;
            }
            TimeUnit.SECONDS.sleep(2);
        }
        runningTime(start, fJobList.size(), true, "");
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
            return phenoList;
        } catch (Exception e) {
            System.err.println("Could not parse a phenotype file.");
            System.exit(1);
        }
        return null;
    }
}
