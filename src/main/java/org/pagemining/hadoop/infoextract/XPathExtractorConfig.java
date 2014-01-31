package org.pagemining.hadoop.infoextract;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class XPathExtractorConfig {
    private List<SiteConfig> sites = new ArrayList<SiteConfig>();

    public XPathExtractorConfig(){
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(XPathExtractorConfig.class.getResourceAsStream("extract.config")));
            SiteConfig site = new SiteConfig();
            while (true){
                String line = reader.readLine();
                if (null == line) break;
                line = line.trim();
                if (line.length() == 0){
                    if(site.getName() != null && site.getName().length() > 0){
                        getSites().add(site);
                        site = new SiteConfig();
                    }
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
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public List<SiteConfig> getSites() {
        return sites;
    }
}
