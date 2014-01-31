package org.pagemining.hadoop.infoextract;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class XPathExtractorConfig {
    private List<SiteConfig> sites = new ArrayList<SiteConfig>();

    public XPathExtractorConfig(){
        SiteConfig site = new SiteConfig();
            for(String line : Config.data){
                line = line.trim();
                if (line.length() == 0){
                    if(site.getName() != null && site.getName().length() > 0){
                        getSites().add(site);
                        site = new SiteConfig();
                    }
                    continue;
                }
                String[] kv = line.split("=");
                String key = kv[0].trim();
                String val = kv[1].trim();
                if(key.equalsIgnoreCase("name")){
                    site.setName(val);
                }
                else if (key.equalsIgnoreCase("pattern")){
                    site.setPattern(val);
                }
                else {
                    site.add(key, val);
                }
            }
            if(site.getName() != null && site.getName().length() > 0){
                getSites().add(site);
            }
    }

    public List<SiteConfig> getSites() {
        return sites;
    }
}
