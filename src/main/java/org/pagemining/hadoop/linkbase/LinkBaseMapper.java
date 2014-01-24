package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class LinkBaseMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = key.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];

        collector.collect(new Text(url), new Text(timestamp));
    }
}

