package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class TestPhoneExtractor {

    public String getDomain(String link){
        try {
            URL url = new URL(link);
            String domain = url.getHost();
            String [] tks = domain.split("\\.");
            if(tks.length < 2) return "other";
            String end = tks[tks.length - 1];
            if(!end.equalsIgnoreCase("com") && !end.equalsIgnoreCase("cn") && !end.equalsIgnoreCase("net")){
                return "other";
            }
            String ret = tks[tks.length - 2] + "." + tks[tks.length - 1];
            if(tks.length >= 3){
                if(ret.equalsIgnoreCase("com.cn") || ret.equalsIgnoreCase("net.cn")){
                    ret = tks[tks.length - 3] + "." + ret;
                }
            }
            return ret;
        } catch (MalformedURLException e) {
            return "other";
        }
    }

    @Test
    public void Test(){
        System.out.println(getDomain("http://www.sina.com.cn/"));
        String html = "<html><body></body><input id=\"pagenum\" value=\"B84DA7BBD3EA6F762555F658AB4CD1F7\" type=\"hidden\" /></html>";
        PhoneExtractor extractor = new PhoneExtractor();
        JSONObject jsonObject = extractor.extract("http://bj.58.com/tech/13478539618433x.shtml", html);
        System.out.println(jsonObject.toString());
    }
}
