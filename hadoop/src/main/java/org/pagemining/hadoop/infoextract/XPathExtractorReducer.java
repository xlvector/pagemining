package org.pagemining.hadoop.infoextract;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.linkbase.LinkInfo;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class XPathExtractorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    HTable table = null;

    @Override
    public void configure(JobConf conf){
        try {
            table = new HTable(conf, "crawler-structured-data");
        } catch (IOException e) {
            return;
        }
    }

    private String rowKey(String buf){
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
        md.update(buf.getBytes());
        String ret = Base64.encodeBase64String(md.digest());
        return ret;
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        try {
            String rowkey = key.toString();
            while (values.hasNext()){
                String value = values.next().toString();
                collector.collect(key, new Text(value));
                Map<String, String> column = new HashMap<String, String>();
                column.put("url", key.toString());
                column.put("data", value);
                HBaseUtil.insert("data", table, rowkey, column);
                break;
            }
        } catch (IOException e){
            return;
        }
    }
}