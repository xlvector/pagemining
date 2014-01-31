package org.pagemining.hadoop.infoextract;

import java.util.HashMap;
import java.util.Map;

public class SiteConfig {
    private String name;
    private String pattern;
    private Map<String, String> attributes = new HashMap<String, String>();
    private Map<String, String> arrays = new HashMap<String, String>();


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
