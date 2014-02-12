package org.pagemining.extractor;

import net.minidev.json.JSONObject;

public interface Extractor {
    public JSONObject extract(String url, String html);
    public JSONObject extract(String url);
}
