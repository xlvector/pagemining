package org.pagemining.extractor.xpath;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.pagemining.extractor.Extractor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XPathExtractor implements Extractor {
    private List<XPathConfig> configs = new ArrayList<XPathConfig>();

    public void AddConfig(XPathConfig config){
        configs.add(config);
    }

    private String extractText(String text, String extractor){
        if(extractor == null){
            return text;
        }
        Pattern p = Pattern.compile(extractor);
        Matcher m = p.matcher(text);
        if(m.find()){
            return m.group(1);
        }
        else{
            return text;
        }
    }

    private Object extractDocumentByStringTemplate(Element doc, String template){
        if(template.charAt(0) == '{' && template.charAt(template.length() - 1) == '}'){
            return template.substring(1, template.length() - 1);
        }
        String [] tks = template.split(",", 3);
        String extractor = null;
        String selector = tks[0].trim();
        if (tks.length > 1){
            extractor = tks[1].trim();
        }
        Elements elements = doc.select(selector);
        if(elements.size() == 0) return null;
        else if(elements.size() == 1) {
            String text = elements.get(0).text();
            if(tks.length == 3){
                text = elements.get(0).attr(tks[2]);
            }
            return extractText(text, extractor);
        }
        else{
            JSONArray jsonArray = new JSONArray();
            for(int i = 0; i < elements.size(); ++i){
                String text = elements.get(i).text();
                if(tks.length == 3){
                    text = elements.get(i).attr(tks[2]);
                }
                jsonArray.add(extractText(text, extractor));
            }
            return jsonArray;
        }
    }

    private Object extractDocument(Element doc, Object template){
        if(template instanceof String){
            return extractDocumentByStringTemplate(doc, (String) (template));
        }
        else if(template instanceof JSONObject){
            JSONObject templateJSON = (JSONObject)template;
            String rootSelector = "html";
            if(templateJSON.containsKey("_root")){
                rootSelector = (String)templateJSON.get("_root");
            }
            Elements elements = doc.select(rootSelector);
            if(elements.size() == 0) return null;
            else {
                JSONArray array = new JSONArray();
                for(int i = 0; i < elements.size(); i++){
                    Element element = elements.get(i);
                    JSONObject elementJson = new JSONObject();
                    for(Map.Entry<String, Object> e : templateJSON.entrySet()){
                        if(e.getKey().isEmpty()) continue;
                        if(e.getKey().charAt(0) == '_') continue;
                        elementJson.put(e.getKey(), extractDocument(element, e.getValue()));
                    }
                    array.add(elementJson);
                }
                if(array.size() == 1){
                    return (JSONObject)(array.get(0));
                }
                else {
                    return array;
                }
            }
        }
        else{
            return null;
        }
    }

    private JSONObject extractDocument(String url, Document doc){
        for(XPathConfig site : configs){
            if(!url.matches(site.getPattern()))
                continue;

            JSONObject root = (JSONObject)extractDocument(doc, site.getJSONObject());
            root.put("_name", site.getName());
            root.put("_pattern", site.getPattern());
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

