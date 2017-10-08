package com.epam.big_data.lab.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileSystemOperations {

    public static List<File> getAllFiles(String pathToDirectory){

        File directory = new File(pathToDirectory);
        File[] fileList = directory.listFiles();
        if(fileList == null){
            return null;
        }
        List<File> listOfFiles = new ArrayList<>();
        Collections.addAll(listOfFiles, fileList);
        return listOfFiles;

    }

    public static void executeCommand(String command) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(command);
        process.waitFor();
        BufferedReader readProc = new BufferedReader(new InputStreamReader(process.getInputStream()));
        while(readProc.ready()) {
            String dataFromBuffer = readProc.readLine();
            System.out.println(dataFromBuffer);
        }
    }

    static String getCurrentDirectory(){
        return System.getProperty("user.dir");
    }

}
