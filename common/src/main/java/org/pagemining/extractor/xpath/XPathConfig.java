package org.pagemining.extractor.xpath;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;

import java.util.HashMap;
import java.util.Map;

public class XPathConfig {
    private JSONObject jsonObject = new JSONObject();

    public void fromString(String buf){
        jsonObject = (JSONObject)JSONValue.parse(buf);
    }

    public String getName() {
        return jsonObject.get("_name").toString();
    }

    public void setName(String name) {
        jsonObject.put("_name", name);
    }

    public String getPattern() {
        return jsonObject.get("_pattern").toString();
    }

    public void setPattern(String pattern) {
        jsonObject.put("_pattern", pattern);
    }

    public JSONObject getJSONObject(){
        return jsonObject;
    }

    public void add(String key, Object value){
        jsonObject.put(key, value);
    }
}
