package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class HiveLinkStatMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String link = tks[1];
        String html = tks[2];

        try{
            URL url = new URL(link);
            collector.collect(new Text(link + "\t" + url.getHost()), new Text(String.valueOf(timestamp)));
        }
        catch (MalformedURLException e){
            return;
        }
    }
}
