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

public class DomainGroupMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public String getDomain(String link){
        try {
            URL url = new URL(link);
            String domain = url.getHost();
            String [] tks = domain.split(".");
            if(tks.length < 2) return "other";
            String end = tks[tks.length - 1];
            if(!end.equalsIgnoreCase("com") && !end.equalsIgnoreCase("cn") && !end.equalsIgnoreCase("net")){
                return "other";
            }
            return tks[tks.length - 2] + "." + tks[tks.length - 1];
        } catch (MalformedURLException e) {
            return "other";
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String url = tks[1];
        String domain = getDomain(url);
        collector.collect(new Text(domain), value);
    }
}
