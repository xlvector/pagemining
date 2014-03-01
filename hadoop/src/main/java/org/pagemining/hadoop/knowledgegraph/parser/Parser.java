package org.pagemining.hadoop.knowledgegraph.parser;

import net.minidev.json.JSONObject;

public interface Parser {
    public Object Build(String buf);
}
