package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.pagemining.extractor.Extractor;

import java.net.MalformedURLException;
import java.net.URL;

public class PhoneExtractor implements Extractor {

    private JSONObject extract58(Document doc, String link){
        Elements elements = doc.select("#pagenum");
        if(elements.size() == 0) return null;
        String pageNumber = elements.get(0).val();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("img_src", "http://image.58.com/showphone.aspx?t=v55&v=" + pageNumber);
        return jsonObject;
    }

    private JSONObject extractGanji(Document doc, String link){
        try{
            URL url = new URL(link);
            Elements elements = doc.select("#isShowPhone > img");
            if(elements.size() == 0) return null;
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("img_src", "http://" + url.getHost() + elements.get(0).attr("src"));
            return jsonObject;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    @Override
    public JSONObject extract(String url, String html) {
        if(url.matches("http://[a-z]+.58.com/[a-z]+/[0-9]+x.shtml")){
            Document doc = Jsoup.parse(html);
            return extract58(doc, url);
        } else if(url.matches("http://[a-z]+.ganji.com/[a-z0-9]+/[a-z0-9]+.htm")){
            Document doc = Jsoup.parse(html);
            return extractGanji(doc, url);
        }
        return null;
    }

    @Override
    public JSONObject extract(String url) {
        return null;
    }
}
