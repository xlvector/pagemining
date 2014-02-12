package org.pagemining.extractor.xpath;

import java.util.HashMap;
import java.util.Map;

public class XPathConfig {
    private String name;
    private String pattern;
    private Map<String, String> attributes = new HashMap<String, String>();
    private Map<String, String> arrays = new HashMap<String, String>();

    public void fromString(String buf){
        String [] lines = buf.split(";");
        for(String line : lines){
            line = line.trim();
            String[] kv = line.split("=", 2);
            if(kv.length != 2) continue;
            String key = kv[0].trim();
            String val = kv[1].trim();
            if(key.equalsIgnoreCase("name")){
                this.setName(val);
            }
            else if (key.equalsIgnoreCase("pattern")){
                this.setPattern(val);
            }
            else {
                this.add(key, val);
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> getArrays() {
        return arrays;
    }

    public void add(String key, String value){
        if (key.charAt(0) == '['){
            this.getArrays().put(key.substring(1, key.length() - 1), value);
        } else {
            this.getAttributes().put(key, value);
        }
    }
}
