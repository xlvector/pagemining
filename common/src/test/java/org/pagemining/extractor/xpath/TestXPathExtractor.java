package org.pagemining.extractor.xpath;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class TestXPathExtractor {
    @Test
    public void Test(){
        XPathConfig config = new XPathConfig();

        String html = "<html>\n" +
                "\t<head>\n" +
                "\t\t<title>Hello World</title>\n" +
                "\t</head>\n" +
                "\t<body>\n" +
                "\t\t<div class=\"hello world\">\n" +
                "\t\t\t<span>aaa</span>\n" +
                "\t\t\t<span>bbb</span>\n" +
                "\t\t</div>\n" +
                "\t\t<div id=\"easy\">\n" +
                "\t\t\tI am happy\n" +
                "\t\t</div>\n" +
                "\t</body>\n" +
                "</html>";

        config.setPattern(".*");
        config.add("[a1]", ".world span");
        config.add("a2", "#easy");
        config.setName("test");
        XPathExtractor extractor = new XPathExtractor();
        extractor.AddConfig(config);
        JSONObject json = extractor.extract("http://pageminig.org", html);

        Assert.assertEquals(json.get("a2").toString(), "I am happy");
        JSONArray a1 = (JSONArray) json.get("a1");
        Assert.assertEquals(a1.get(0).toString(), "aaa");
        Assert.assertEquals(a1.get(1).toString(), "bbb");
    }

    @Test
    public void TestLink(){
        XPathConfig config = new XPathConfig();
        config.setPattern(".*");
        config.add("address",".field-group:contains(地址)");
        config.setName("test");
        XPathExtractor extractor = new XPathExtractor();
        extractor.AddConfig(config);
        JSONObject json = extractor.extract("http://sh.meituan.com/shop/1486684");
        System.out.println();
    }
}
