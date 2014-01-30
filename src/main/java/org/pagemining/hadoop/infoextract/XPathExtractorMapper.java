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

public class XPathExtractorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String [] tks = value.toString().split("\t");
        if(tks.length != 3) return;

        String timestamp = tks[0];
        String url = tks[1];
        String html = tks[2];

        if(!url.matches("http://www.dianping.com/shop/[0-9]+/")
            && !url.matches("http://www.dianping.com/shop/[0-9]+"))
            return;

        Document doc = Jsoup.parse(html);
        Elements breadcrumb = doc.select("div.breadcrumb");
        if(breadcrumb.size() == 0) return;
        Elements regions = breadcrumb.get(0).select("span.bread-name");
        JSONObject root = new JSONObject();
        JSONArray regionArray = new JSONArray();
        for(int i = 0; i < regions.size(); ++i){
            regionArray.add(regions.get(i).text());
        }
        root.put("regions", regionArray);

        Elements title = doc.select("h1.shop-title");
        if(title.size() > 0){
            root.put("title", title.get(0).text());
        }

        Elements price = doc.select(".stress");
        if(price.size() > 0){
            root.put("price", price.get(0).text());
        }
        collector.collect(new Text(url), new Text(root.toJSONString()));
    }
}
