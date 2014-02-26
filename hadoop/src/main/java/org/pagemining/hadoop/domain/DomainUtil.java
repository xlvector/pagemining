package org.pagemining.hadoop.domain;

import java.net.MalformedURLException;
import java.net.URL;

public class DomainUtil {
    public static String getDomain(String link){
        try {
            URL url = new URL(link);
            String domain = url.getHost();
            return domain;
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static String getFileNameByDomain(String domain){
        if(domain.endsWith(".com")){
            return "com";
        }
        else if(domain.endsWith(".net")){
            return "net";
        }
        else if(domain.endsWith(".cn")){
            if(domain.endsWith(".gov.cn")){
                return "gov.cn";
            }
            else if(domain.endsWith(".edu.cn")){
                return "edu.cn";
            }
            else if(domain.endsWith(".org.cn")){
                return "org.cn";
            }
            else {
                return "cn";
            }
        }
        else {
            return "other";
        }
    }
}
