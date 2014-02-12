package org.pagemining.extractor.xpath;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.pagemining.extractor.Extractor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XPathExtractor implements Extractor {
    private List<XPathConfig> configs = new ArrayList<XPathConfig>();

    public void AddConfig(XPathConfig config){
        configs.add(config);
    }

    private JSONObject extractDocument(String url, Document doc){
        for(XPathConfig site : configs){
            if(!url.matches(site.getPattern()))
                continue;

            JSONObject root = new JSONObject();
            root.put("name", site.getName());
            root.put("pattern", site.getPattern());
            for(Map.Entry<String, String> e : site.getAttributes().entrySet()){
                Elements elements = doc.select(e.getValue());
                if(elements.size() == 0) continue;
                root.put(e.getKey(), elements.get(0).text());
            }
            for(Map.Entry<String, String> e : site.getArrays().entrySet()){
                Elements elements = doc.select(e.getValue());
                JSONArray array = new JSONArray();
                for(int i = 0; i < elements.size(); ++i){
                    array.add(elements.get(i).text());
                }
                root.put(e.getKey(), array);
            }
            return root;
        }
        return null;
    }

    @Override
    public JSONObject extract(String url, String html) {
        Document doc = Jsoup.parse(html);
        return extractDocument(url, doc);
    }

    @Override
    public JSONObject extract(String url) {
        try {
            Document doc = Jsoup.connect(url)
                    .userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
                    .get();
            return extractDocument(url, doc);
        } catch (IOException e) {
            return null;
        }
    }
}

