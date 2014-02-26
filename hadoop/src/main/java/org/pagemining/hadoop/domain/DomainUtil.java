package org.pagemining.hadoop.domain;

import java.net.MalformedURLException;
import java.net.URL;

public class DomainUtil {
    public static String getDomain(String link){
        try {
            URL url = new URL(link);
            String domain = url.getHost().toLowerCase();
            String [] tks = domain.split("\\.");
            if(tks.length >= 2 && (domain.endsWith(".com") || domain.endsWith(".net"))){
                return tks[tks.length - 2] + "." + tks[tks.length - 1];
            }
            if(domain.endsWith(".cn")){
                if(domain.endsWith(".com.cn") || domain.endsWith(".gov.cn") || domain.endsWith(".org.cn") || domain.endsWith(".edu.cn") || domain.endsWith(".net.cn")){
                    if(tks.length >= 3){
                        return tks[tks.length - 3] + "." + tks[tks.length - 2] + "." + tks[tks.length - 1];
                    }
                } else {
                    if(tks.length >= 2){
                        return tks[tks.length - 2] + "." + tks[tks.length - 1];
                    }
                }
            }
            return "other";
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static String getTopDomain(String domain){
        String [] tks = domain.split("\\.");
            if((domain.endsWith(".com") || domain.endsWith(".net"))){
                return tks[tks.length - 1];
            }
            if(domain.endsWith(".cn")){
                if(domain.endsWith(".com.cn") || domain.endsWith(".gov.cn") || domain.endsWith(".org.cn") || domain.endsWith(".edu.cn") || domain.endsWith(".net.cn")){
                    return tks[tks.length - 2] + "." + tks[tks.length - 1];
                } else {
                    return "cn";
                }
            }
            return "other";
    }
}
