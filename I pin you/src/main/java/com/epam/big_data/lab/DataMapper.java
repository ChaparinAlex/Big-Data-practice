package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.CustomKey;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class DataMapper extends Mapper<LongWritable, Text, CustomKey, Text>{

    private FileSystem fs;
    private Map<String, String> idAndCityNamesNotebook;

    @Override
    protected void setup(Mapper<LongWritable, Text, CustomKey, Text>.Context context) throws IOException, InterruptedException{
        fs = FileSystem.get(context.getConfiguration());
        idAndCityNamesNotebook = new HashMap<>();
        URI[] cacheFiles = context.getCacheFiles();
        if(cacheFiles != null) {
            for(URI uri : cacheFiles) {
                fillIdAndCityNamesNotebook(new Path(uri.getPath()));
            }
        }
    }

    private void fillIdAndCityNamesNotebook(Path filePath) throws IOException {
        FSDataInputStream is = fs.open(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String idAndCity;
        while((idAndCity = bufferedReader.readLine()) != null) {
            String[] sections = idAndCity.split("\t");
            if(sections.length == 2){
                idAndCityNamesNotebook.put(sections[0], sections[1]);
            }
        }
        is.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
                                                                              throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split("\t");
            Long biddingPrice = null;
            try{
                biddingPrice = Long.parseLong(sections[19]);
            }catch (NumberFormatException e){
                e.printStackTrace();
            }
            if(biddingPrice == null || biddingPrice <= 250){
                continue;
            }
            String operatingSystemName = UserAgent.parseUserAgentString(sections[4]).getOperatingSystem().getName();
            context.getCounter("Operating systems", operatingSystemName).increment(1);
            String cityName = idAndCityNamesNotebook.get(sections[7]);
            if(cityName != null){
                context.write(new CustomKey(new Text(cityName), new Text(operatingSystemName)), new Text("1"));
            }else{
                String regionName = idAndCityNamesNotebook.get(sections[6]);
                context.write(new CustomKey(new Text(regionName), new Text(operatingSystemName)), new Text("1"));
            }

        }

    }
}
