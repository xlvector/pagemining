package org.pagemining.hadoop.infoextract;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class XPathConfigReader {
    public static String readConfig(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;
        StringBuilder sb = new StringBuilder();
        while ((line = reader.readLine()) != null){
            sb.append(line);
            sb.append("\n");
        }
        reader.close();
        return sb.toString();
    }
}
