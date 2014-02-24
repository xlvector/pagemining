package org.pagemining.hadoop.phone;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.infoextract.HBaseUtil;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PhoneExtractorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        while (values.hasNext()){
            String value = values.next().toString();
            collector.collect(key, new Text(value));
            break;
        }
    }
}
