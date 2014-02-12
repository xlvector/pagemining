package org.pagemining.extractor.xpath;

import org.junit.Assert;
import org.junit.Test;

public class TestXPathConfig {
    @Test
    public void Test(){
        XPathConfig config = new XPathConfig();
        config.add("[a]", "#aaa");
        config.add("b", "#bbb");
        Assert.assertEquals(config.getAttributes().get("b"), "#bbb");
        Assert.assertEquals(config.getArrays().get("a"), "#aaa");

        config.setPattern("http://[a-z0-9]+.pageming.org/.*.html");
        Assert.assertEquals(config.getPattern(), "http://[a-z0-9]+.pageming.org/.*.html");
    }

}
