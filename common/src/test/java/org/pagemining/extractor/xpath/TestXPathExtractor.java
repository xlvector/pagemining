package org.pagemining.extractor.xpath;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class TestXPathExtractor {
    @Test
    public void Test(){

        String html = "<html>\n" +
                "\t<head>\n" +
                "\t\t<title>Hello World</title>\n" +
                "\t</head>\n" +
                "\t<body>\n" +
                "\t\t<div class=\"hello world\">\n" +
                "\t\t\t<span><div class=\"c1\">aa1</div><div class=\"c2\">aa2</div></span>\n" +
                "\t\t\t<span><div class=\"c1\">bb1</div><div class=\"c2\">bb2</div></span>\n" +
                "\t\t</div>\n" +
                "\t\t<div id=\"easy\">\n" +
                "\t\t\tI am happy 18665108545\n" +
                "\t\t</div>\n" +
                "\t</body>\n" +
                "</html>";

        String template = "{\n" +
                "\t\"_name\" : \"test\",\n" +
                "\t\"_pattern\" : \".*\",\n" +
                "\t\"a1\" : {\n" +
                "\t\t\"_root\" : \".world span\",\n" +
                "\t\t\"c1\" : \".c1\",\n" +
                "\t\t\"c2\" : \".c2\"\n" +
                "\t},\n" +
                "\t\"a2\" : \"#easy, ([0-9]+)\"\n" +
                "}";
        XPathConfig config = new XPathConfig();
        config.fromString(template);
        XPathExtractor extractor = new XPathExtractor();
        extractor.AddConfig(config);
        JSONObject json = extractor.extract("http://pageminig.org", html);

        Assert.assertEquals(json.get("a2").toString(), "18665108545");
        JSONArray a1 = (JSONArray) json.get("a1");
        Assert.assertEquals(((JSONObject)a1.get(0)).get("c1").toString(), "aa1");
        Assert.assertEquals(((JSONObject)a1.get(1)).get("c2").toString(), "bb2");
    }

    @Test
    public void TestLink(){
        XPathConfig config = new XPathConfig();
        config.fromString("{\"_name\":\"dianping-shop\",\"_pattern\":\".*\",\"links\":\"[href]\"}");
        XPathExtractor extractor = new XPathExtractor();
        extractor.AddConfig(config);
        JSONObject json = extractor.extract("http://sh.meituan.com/shop/1486684");
        System.out.println();
    }
}
