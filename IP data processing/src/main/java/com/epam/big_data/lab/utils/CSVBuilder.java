package com.epam.big_data.lab.utils;

import java.io.*;

class CSVBuilder {

    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";

    private static void write(FileWriter fw, String... values) throws IOException {
        for(int i = 0; i < values.length; i++){
            fw.append(values[i]);
            if(i != values.length - 1){
                fw.append(COMMA_DELIMITER);
            }else{
                fw.append(NEW_LINE_SEPARATOR);
            }
        }
    }

    static void convertFileToCSV(String srcFilePath, String dstFilePath) throws IOException {
        FileWriter fw = new FileWriter(dstFilePath);
        BufferedReader br = new BufferedReader(new FileReader(srcFilePath));
        String line;

        while ((line = br.readLine()) != null) {
           String[] values = line.split("[\\s|,]");
           write(fw, values);
        }

        br.close();
        fw.close();
    }

}
