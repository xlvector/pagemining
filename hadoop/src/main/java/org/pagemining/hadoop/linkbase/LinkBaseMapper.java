package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LinkBaseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private Map<String, Integer> cache = new HashMap<String, Integer>();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];
        String html = tks[2];

        Document doc = Jsoup.parse(html);
        Elements links = doc.select("a[href]");

        for (Element link : links) {
            String subLink = link.attr("abs:href");
            if(!cache.containsKey(subLink)){
                cache.put(subLink, 1);
            }
            else {
                cache.put(subLink, 1 + cache.get(subLink));
            }
        }
        if(cache.size() > 100000){
            for(Map.Entry<String, Integer> e : cache.entrySet()){
                collector.collect(new Text(e.getKey()), new Text(String.valueOf(e.getValue())));
            }
            cache = new HashMap<String, Integer>();
        }
    }
}

