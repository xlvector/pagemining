package org.pagemining.extractor.html;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.pagemining.extractor.Extractor;

public class HTMLExtractor implements Extractor {
    @Override
    public JSONObject extract(String url, String html) {
        Document doc = Jsoup.parse(html);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("title", doc.title());
        Elements h1s = doc.getElementsByTag("h1");
        JSONArray h1Array = new JSONArray();
        for(int i = 0; i < h1s.size(); i++){
            Element h1 = h1s.get(i);
            h1Array.add(h1.text());
        }
        jsonObject.put("h1", h1Array);

        Elements anchors = doc.getElementsByTag("a");
        JSONArray anchorArray = new JSONArray();
        for(int i = 0; i < anchors.size(); i++){
            Element anchor = anchors.get(i);
            String anchorText = anchor.text();
            String href = anchor.attr("href");
            String alt = anchor.attr("alt");
            JSONObject linkObj = new JSONObject();
            linkObj.put("text", anchorText);
            linkObj.put("href", href);
            linkObj.put("alt", alt);
            anchorArray.add(linkObj);
        }
        jsonObject.put("a", anchorArray);
        return jsonObject;
    }

    @Override
    public JSONObject extract(String url) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
