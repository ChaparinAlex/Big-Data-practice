package com.epam.big_data.lab.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class FileSystemOperations {

    static List<File> getAllFiles(String pathToDirectory){

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

    public static String getCurrentDirectory(){
        return System.getProperty("user.dir");
    }

    public static String getNameOfJarFile(){
        List<File> fileList = getAllFiles(getCurrentDirectory());
        fileList = fileList.stream().filter(file -> file.getName().endsWith(".jar")).collect(Collectors.toList());
        Comparator<File> comparator = Comparator.comparing(File::lastModified);
        fileList.sort(comparator.reversed());
        return fileList.stream().limit(1).collect(Collectors.toList()).get(0).getName();
    }

}
