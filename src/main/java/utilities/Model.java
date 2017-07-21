package utilities;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class Model {

    // Writes model to a file.
    public static void setModel(String outPath, String model) throws IOException {
        Path path = new Path("hdfs:" + outPath);
        FileSystem fs = FileSystem.get(new Configuration());

        // Write new x to new file.
        if (fs.exists(path)) {
            BufferedReader buffIn = new BufferedReader(new InputStreamReader(fs.open(path)));
            BufferedWriter buffOut = new BufferedWriter(new OutputStreamWriter(fs.append(path)));
            if (buffIn.readLine() == "") {
                buffOut.write(model);    
            } else {
                buffOut.write("\t" + model);
            }
            buffIn.close();
            buffOut.close();      
        } else {
            BufferedWriter buffOut = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
            buffOut.write(model);
            buffOut.close();   
        }        
    }

    public static String getModel(Configuration conf) {
        String model = conf.get("model");
        if (!".".equals(model)) {
            return model;
        } else {
            return "";
        }
    }
}