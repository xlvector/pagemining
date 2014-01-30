package org.pagemining.hadoop.infoextract;


import org.junit.Test;

public class RegexTest {
    @Test
    public void Test(){
        String url = "http://www.dianping.com/shop/123456/";
        System.out.println(url.matches("http://www.dianping.com/shop/[0-9]+/"));
    }
}
