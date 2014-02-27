package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.junit.Test;
import org.pagemining.hadoop.domain.DomainUtil;
import org.pagemining.hadoop.infoextract.HBaseUtil;

import java.net.MalformedURLException;
import java.net.URL;

public class TestPhoneExtractor {

    @Test
    public void Test(){
        System.out.println(HBaseUtil.getEndRowKey("http://www.sina.com.cn/a.html", 48));
        String html = "<html><body><input id=\"pagenum\" value=\"B84DA7BBD3EA6F762555F658AB4CD1F7\" type=\"hidden\" /></body><script>var www = 1;</script></html>";
        Document doc = Jsoup.parse(html);
        //Elements scripts = doc.select("script");
        //scripts.get(0).remove();
        System.out.println(DomainUtil.cleanHtml(doc.html()));
        PhoneExtractor extractor = new PhoneExtractor();
        JSONObject jsonObject = extractor.extract("http://bj.58.com/tech/13478539618433x.shtml", html);
        System.out.println(jsonObject.toString());
    }
}
