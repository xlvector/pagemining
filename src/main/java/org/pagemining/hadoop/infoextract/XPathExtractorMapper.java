package org.pagemining.hadoop.infoextract;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
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
import org.pagemining.hadoop.linkbase.LinkInfo;

import java.io.IOException;
import java.util.Map;

public class XPathExtractorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private XPathExtractorConfig config = null;

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        if(config == null){
            config = new XPathExtractorConfig();
        }
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];
        String html = tks[2];

        Document doc = Jsoup.parse(html);

        for(SiteConfig site : config.getSites()){
            if(!url.matches(site.getPattern()))
                continue;

            JSONObject root = new JSONObject();
            for(Map.Entry<String, String> e : site.getAttributes().entrySet()){
                Elements elements = doc.select(e.getKey());
                if(elements.size() == 0) continue;
                root.put(e.getKey(), elements.get(0).text());
            }
            for(Map.Entry<String, String> e : site.getArrays().entrySet()){
                Elements elements = doc.select(e.getKey());
                JSONArray array = new JSONArray();
                for(int i = 0; i < elements.size(); ++i){
                    array.add(elements.get(i).text());
                }
                root.put(e.getKey(), array);
            }
            collector.collect(new Text(url), new Text(root.toJSONString()));
        }
    }
}
