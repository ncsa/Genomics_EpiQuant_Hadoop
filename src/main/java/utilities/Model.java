package utilities;

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
    public static void setModel(String outPath, String model) throws IOException {
        Path path = new Path(outPath);
        FileSystem fs = FileSystem.get(new Configuration());
        // Write new x to new file.
        path = new Path("hdfs:" + outPath);
        BufferedWriter buffOut = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
        buffOut.write(model);
        buffOut.close();           
    }

    // Update model file.
    public static void updateModel(String inPath, String outPath) throws IOException {
        Path path = new Path("hdfs:" + inPath);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(fs.open(path)));

        String line = buffIn.readLine();
        buffIn.close();

        path = new Path("hdfs:" + outPath);
        BufferedWriter buffOut = new BufferedWriter(new OutputStreamWriter(fs.append(path)));
        if (line != "") {
            buffOut.write("\t" + line);
        }
        buffOut.close();
    }
}