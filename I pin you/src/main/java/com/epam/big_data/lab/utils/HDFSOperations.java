package com.epam.big_data.lab.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class HDFSOperations {

    private FileSystem hdfs;
    private Path homeDir;

    public HDFSOperations(Configuration conf){
        try {
            hdfs = FileSystem.get(conf);
        }
        catch (IOException e){
            e.printStackTrace();
        }
        homeDir = hdfs.getHomeDirectory();
    }

    public void setUp(Path hdfsInputFolderPath, Path hdfsOutputFolderPath) throws IOException {
        if(folderOrFileExists(hdfsOutputFolderPath)){
            deleteDirectory(hdfsOutputFolderPath);
        }
        copyFilesToHdfs(hdfsInputFolderPath);
    }

    public String getHdfsHomeFolder() throws IOException {
        return homeDir.toString();
    }

    private void deleteDirectory(Path folderPath) throws IOException {
        hdfs.delete(folderPath, true);
    }

    private boolean folderOrFileExists(Path path) throws IOException {
        return hdfs.exists(path);
    }

    private void copyFilesToHdfs(Path newFolderPath) throws IOException {

        Path localDirectory = new Path(FileSystemOperations.getCurrentDirectory());
        List<File> fileList = FileSystemOperations.getAllFiles(localDirectory.toString());
        if(fileList == null){
            return;
        }
        for(File file : fileList){
            if(file.getName().endsWith(".jar")){
                continue;
            }
            Path hdfsFilePath = new Path(newFolderPath + "/" + file.getName());
            if(!folderOrFileExists(hdfsFilePath)){
                hdfs.copyFromLocalFile(new Path(file.getCanonicalPath()), hdfsFilePath);
            }
        }

    }


}
