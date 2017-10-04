package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.FileSystemOperations;
import com.epam.big_data.lab.utils.HDFSOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataDriver extends Configured implements Tool {

    private static final String HDFS_RELATIVE_INPUT_PATH = "ip_data_processing/input";
    private static final String HDFS_RELATIVE_OUTPUT_PATH = "ip_data_processing/output";


    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 2) {
            System.out.println("To run the program please use following number of arguments: \n0 - " +
                    "your HDFS input and output directories will be created automatically and they will obtain the next " +
                    "paths: <hdfsHomeFolder>/ip_data_processing/input and <hdfsHomeFolder>/ip_data_processing/output;" +
                    "\n2 (i/o HDFS-paths) - you must type input and output HDFS directories manually");
            System.out.println("Your current arguments: ");
            for (String arg : args) {
                System.out.println(arg);
            }
            System.out.println("Please, restart with proper number of arguments!");
            return;
        }
        int res = ToolRunner.run(new DataDriver(), args);
        System.out.println("Results of MapReduce task (content of output.csv):");
        FileSystemOperations.executeCommand("cat output.csv");
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HDFSOperations hdfsOperations = new HDFSOperations(conf);
        String hdfsInputFolder, hdfsOutputFolder;

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
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFolder));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(DataCombiner.class);

        job.setReducerClass(DataReducer.class);

        conf.set("mapred.textoutputformat.separatorText", ",");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        int res = job.waitForCompletion(true) ? 0 : 1;

        hdfsOperations.cleanUp(new Path(hdfsOutputFolder + "/part-r-00000"),
                new Path(FileSystemOperations.getCurrentDirectory() + "/part-r-00000"),
                new Path(FileSystemOperations.getCurrentDirectory() + "/output.csv"));

        return res;
    }
}

