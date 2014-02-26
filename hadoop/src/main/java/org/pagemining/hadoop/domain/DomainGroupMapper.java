package org.pagemining.hadoop.domain;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class DomainGroupMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs mos;
    private OutputCollector<Text, Text> output;

    @Override
    public void configure(JobConf conf){
        mos = new MultipleOutputs(conf);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String url = tks[1];
        String domain = DomainUtil.getDomain(url);
        if(domain != null){
            String filename = DomainUtil.getFileNameByDomain(domain);
            output = mos.getCollector(filename, reporter);
            output.collect(new Text(domain), value);
        }
    }
}
