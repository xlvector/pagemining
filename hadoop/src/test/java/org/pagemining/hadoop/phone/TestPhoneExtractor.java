package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import org.junit.Test;

public class TestPhoneExtractor {
    @Test
    public void Test(){
        String html = "<html><body></body><input id=\"pagenum\" value=\"B84DA7BBD3EA6F762555F658AB4CD1F7\" type=\"hidden\" /></html>";
        PhoneExtractor extractor = new PhoneExtractor();
        JSONObject jsonObject = extractor.extract("http://bj.58.com/tech/13478539618433x.shtml", html);
        System.out.println(jsonObject.toString());
    }
}
