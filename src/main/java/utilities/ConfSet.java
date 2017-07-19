package utilities;

import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;

public class ConfSet {
    // Gets the y value from the context configuration.
    public static String getY(Configuration conf) {
        String[] yIn = conf.get("y").split("\\t");
        String yOut = yIn[0];
        for (int i = 1 ; i < yIn.length; i++) {
            yOut += "," + yIn[i];
        }
        return yOut;
    }

    public static double[] convertY(String yString) {
        String[] yStrings = yString.split(",");
        double[] y = new double[yStrings.length - 1];
        for (int i = 1; i < yStrings.length; i++) {
            y[i - 1] = Double.parseDouble(yStrings[i]);
        }
        return y;
    }

    // Gets the x values from the context configuration.
    public static String[][] getX(Configuration conf) {
        String[] xs = conf.get("x").split(":");
        String[][] xsArray = new String[xs.length][];
        for (int i = 0; i < xs.length; i++) {
            xsArray[i] = xs[i].split(",");
        }
        return xsArray;
    }

    // Convert the x values from strings to doubles.
    public static double[][] convertX(String[][] xStrings) {
        double[][] xValues = new double[xStrings.length - 1][xStrings[0].length - 1];
        for (int i = 1; i < xStrings.length; i++) {
            for (int j = 1; j < xStrings[0].length; i++) {
                xValues[i - 1][j - 1] = Double.parseDouble(xStrings[i][j]);
            }
        }
        return xValues;
    }

    // Gets the x value labels from the context configuration.
    public static Set<String> storeX(String[][] xStrings) {
        Set<String> xSet = new HashSet<String>();
        for (int i = 0; i < xStrings.length; i++) {
            xSet.add(xStrings[i][0]);
        }
        return xSet;
    }
}