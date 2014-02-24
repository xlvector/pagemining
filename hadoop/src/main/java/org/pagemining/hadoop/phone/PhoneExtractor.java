package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.pagemining.extractor.Extractor;

public class PhoneExtractor implements Extractor {

    private JSONObject extract58(Document doc){
        Elements elements = doc.select("#pagenum");
        if(elements.size() == 0) return null;
        String pageNumber = elements.get(0).val();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("img_src", "http://image.58.com/showphone.aspx?t=v55&v=" + pageNumber);
        return jsonObject;
    }

    @Override
    public JSONObject extract(String url, String html) {
        Document doc = Jsoup.parse(html);
        if(url.matches("http://[a-z]+.58.com/[a-z]+/[0-9]+x.shtml")){
            return extract58(doc);
        }
        return null;
    }

    @Override
    public JSONObject extract(String url) {
        return null;
    }
}
