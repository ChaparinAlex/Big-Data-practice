package com.epam.big_data.lab.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileSystemOperations {

    private static PrintStream console;

    public static PrintStream getConsole(){
        return console;
    }

    static List<File> getAllFilesWithExtension(String pathToDirectory, String... extensions){

        File directory = new File(pathToDirectory);
        File[] fileList = directory.listFiles();
        if(fileList == null){
            return null;
        }
        List<File> listOfFiles = new ArrayList<>();
        for(File file : fileList){
            for(String extension : extensions){
                if(file.getName().endsWith(extension)){
                    listOfFiles.add(file);
                }
            }
        }
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

    public static void writeLogsToFile() throws IOException {
        String localFilePath = getCurrentDirectory() + "/console_logs.log";
        File file = new File(localFilePath);
        if(!file.exists()){
            file.createNewFile();
        }
        console = System.out;
        FileOutputStream fos = new FileOutputStream(file);
        PrintStream ps = new PrintStream(fos);
        System.setOut(ps);
    }
}
