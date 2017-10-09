package com.epam.big_data.lab;

import com.epam.big_data.lab.mappers.CityAndRegionMapper;
import com.epam.big_data.lab.mappers.DataMapper;
import com.epam.big_data.lab.utils.CustomKey;
import com.epam.big_data.lab.utils.FileSystemOperations;
import com.epam.big_data.lab.utils.HDFSOperations;
import com.epam.big_data.lab.utils.KeyPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class AppDriver extends Configured implements Tool {

    private static final String HDFS_RELATIVE_INPUT_PATH = "i_pin_you/input";
    private static final String HDFS_RELATIVE_OUTPUT_PATH = "i_pin_you/output";
    private static String hdfsInputFolder;
    private static String hdfsOutputFolder;


    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 2) {
            System.out.println("To run the program please use following number of arguments: \n0 - " +
                    "your HDFS input and output directories will be created automatically and they will obtain the next " +
                    "paths: <hdfsHomeFolder>/i_pin_you/input and <hdfsHomeFolder>/i_pin_you/output;" +
                    "\n2 (i/o HDFS-paths) - you must type input and output HDFS directories manually");
            System.out.println("Your current arguments: ");
            for (String arg : args) {
                System.out.println(arg);
            }
            System.out.println("Please, restart with proper number of arguments!");
            return;
        }
        int res = ToolRunner.run(new AppDriver(), args);
        System.out.println("Results of MapReduce task:");
        FileSystemOperations.executeCommand("hdfs dfs -cat " + hdfsOutputFolder + "/part-r*");
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HDFSOperations hdfsOperations = new HDFSOperations(conf);

        if(args.length == 2){
            hdfsInputFolder = args[0];
            hdfsOutputFolder = args[1];
        }else{
            hdfsInputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_INPUT_PATH;
            hdfsOutputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_OUTPUT_PATH;
        }

        hdfsOperations.setUp(new Path(hdfsInputFolder), new Path(hdfsOutputFolder));

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName("I pin you");

        List<String> inputFileList = hdfsOperations.getAllFileNamesFromDirectory(new Path(hdfsInputFolder));
        if(inputFileList != null && !inputFileList.isEmpty()){
            for(String fileName : inputFileList){
                if(fileName.equals("region.en.txt") || fileName.equals("city.en.txt")){
                    MultipleInputs.addInputPath(job, new Path(hdfsInputFolder + "/" + fileName),
                            TextInputFormat.class, CityAndRegionMapper.class);
                }else{
                    MultipleInputs.addInputPath(job, new Path(hdfsInputFolder + "/" + fileName),
                            TextInputFormat.class, DataMapper.class);
                }
            }
        }

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(hdfsInputFolder));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setReducerClass(DataReducer.class);

        job.setPartitionerClass(KeyPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}

