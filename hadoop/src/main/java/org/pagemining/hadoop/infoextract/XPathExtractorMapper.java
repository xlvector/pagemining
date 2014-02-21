package org.pagemining.hadoop.infoextract;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.pagemining.extractor.xpath.XPathConfig;
import org.pagemining.extractor.xpath.XPathExtractor;

import java.io.IOException;
import java.util.Map;

public class XPathExtractorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private XPathExtractor extractor = null;

    @Override
    public void configure(JobConf job){
        extractor = new XPathExtractor();
        String configStr = job.get("xpath.config");
        JSONArray jsonArray = (JSONArray)JSONValue.parse(configStr);
        for(int i = 0; i < jsonArray.size(); ++i){
            XPathConfig config = new XPathConfig();
            config.fromString(((JSONObject)jsonArray.get(i)).toString());
            extractor.AddConfig(config);
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];
        String html = tks[2];

        JSONObject jsonDoc = extractor.extract(url, html);
        if(jsonDoc != null){
            jsonDoc.put("_crawled_at", timestamp);
            collector.collect(new Text(url), new Text(jsonDoc.toString()));
        }
    }
}
