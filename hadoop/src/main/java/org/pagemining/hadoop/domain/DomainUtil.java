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
                return "govcn";
            }
            else if(domain.endsWith(".edu.cn")){
                return "educn";
            }
            else if(domain.endsWith(".org.cn")){
                return "orgcn";
            }
            else {
                return "cn";
            }
        }
        else {
            return "other";
        }
    }

    public static String[] getDomainEnds(){
        return new String[]{"com", "net", "govcn", "educn", "orgcn", "cn", "other"};
    }
}
