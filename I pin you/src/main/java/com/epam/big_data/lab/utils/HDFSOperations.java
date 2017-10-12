package com.epam.big_data.lab.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
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

    public void setUp(Path hdfsInputFolderPath, Path hdfsOutputFolderPath, Path hdfsDCacheFolderPath) throws IOException {
        if(folderOrFileExists(hdfsOutputFolderPath)){
            deleteDirectory(hdfsOutputFolderPath);
        }
        copyInputDataFilesToHdfs(hdfsInputFolderPath);
        if(!folderOrFileExists(hdfsDCacheFolderPath)){
            createDirectory(hdfsDCacheFolderPath);
        }
        copyDCacheFilesToHdfs(hdfsDCacheFolderPath);
    }

    public List<Path> getAllFilesFromDirectory(Path pathToDirectory) throws IOException, URISyntaxException {
        List<Path> pathList = new ArrayList<>();
        FileStatus[] fileStatus = hdfs.listStatus(pathToDirectory);
        for(FileStatus status : fileStatus){
            pathList.add(status.getPath());
        }
        return pathList;
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

    private void createDirectory(Path pathToDirectory) throws IOException {
        hdfs.mkdirs(pathToDirectory);
    }

    private void copyInputDataFilesToHdfs(Path newFolderPath) throws IOException {

        Path localDirectory = new Path(FileSystemOperations.getCurrentDirectory());
        List<File> fileList = FileSystemOperations.getAllFiles(localDirectory.toString());
        if(fileList == null){
            return;
        }
        for(File file : fileList){
            if(file.getName().endsWith(".jar") || file.getName().contains("dcache")){
                continue;
            }
            Path hdfsFilePath = new Path(newFolderPath + "/" + file.getName());
            if(!folderOrFileExists(hdfsFilePath)){
                hdfs.copyFromLocalFile(new Path(file.getCanonicalPath()), hdfsFilePath);
            }
        }

    }

    private void copyDCacheFilesToHdfs(Path newFolderPath) throws IOException {

        Path localDirectory = new Path(FileSystemOperations.getCurrentDirectory());
        List<File> fileList = FileSystemOperations.getAllFiles(localDirectory.toString());
        if(fileList == null){
            return;
        }
        for(File file : fileList){
            if(file.getName().contains("dcache")){
                Path hdfsDCacheFilePath = new Path(newFolderPath + "/" + file.getName());
                if(!folderOrFileExists(hdfsDCacheFilePath)){
                    hdfs.copyFromLocalFile(new Path(file.getCanonicalPath()), hdfsDCacheFilePath);
                }
            }

        }

    }


}
