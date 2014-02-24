package org.pagemining.hadoop.phone;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.extractor.xpath.XPathConfig;
import org.pagemining.extractor.xpath.XPathExtractor;

import java.io.IOException;

public class PhoneExtractorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private PhoneExtractor extractor = new PhoneExtractor();



    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];
        String html = tks[2];

        JSONObject jsonDoc = extractor.extract(url, html);
        if(jsonDoc != null){
            collector.collect(new Text(url), new Text(jsonDoc.toString()));
        }
    }
}
