package utilities;

import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class Model {
    // Reads in model from a file.
    public static String getModel(String inPath) throws IOException {
        Path path = new Path("hdfs:" + inPath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
        String out = buff.readLine();
        buff.close();
        return out;
    }

    // Writes model to a file.
    public static void setModel(String inPath, String outPath) throws IOException {
        // Open reader to get new x.
        Path path = new Path("hdfs:" + inPath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(fs.open(path)));

        // Separate x from other data.
        String line = buffIn.readLine();
        buffIn.close();
        String tokens[] = line.split("\\t");

        // Write new x to new file.
        path = new Path("hdfs:" + outPath);
        BufferedWriter buffOut = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
        buffOut.write(tokens[2]);
        buffOut.close();           
    }
}