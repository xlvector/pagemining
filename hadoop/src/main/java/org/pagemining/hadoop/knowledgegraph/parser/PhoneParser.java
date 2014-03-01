package org.pagemining.hadoop.knowledgegraph.parser;

public class PhoneParser implements Parser {

    @Override
    public Object Build(String buf) {
        return buf.replaceAll(" ", "");
    }
}
