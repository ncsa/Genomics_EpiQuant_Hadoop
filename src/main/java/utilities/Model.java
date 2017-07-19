package utilities;

import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class Model {
    // Reads in model from a file.
    public static String getModel(String inPath) throws IOException {
        try {
            Path path = new Path("hdfs:" + inPath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));
            String out = buff.readLine();
            buff.close();
            return out;
        } catch (Exception e) {
            System.err.println("Could not parse the model file.");
            System.exit(1);
        }
        return null;
    }

    // Writes model to a file.
    public static void setModel() {

    }
}