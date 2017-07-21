package utilities;

import org.apache.hadoop.conf.Configuration;

public class ConfSet {

    public static String getXNewString(String[] tokens) {
        String xOut = tokens[0];
        for (int i = 1; i < tokens.length; i++) {
            xOut += "," + tokens[i];
        }
        return xOut;
    }

    public static double[][] combineX(String[] newTokens, double[][] xModel) {
        for (int i = 0; i < xModel.length; i++) {
            xModel[i][xModel[0].length - 1] = Double.parseDouble(newTokens[i + 1]);
        }
        return xModel;
    }

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

    public static String getModel(Configuration conf) {
        String model = conf.get("model");
        if (!model.equals("")) {
            return model;
        } else {
            return "";
        }
    }
}