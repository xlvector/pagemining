package org.pagemining.hadoop.dict;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class DictExtractorMapper extends TableMapper<Text, Text> {
    private String encode(String buf) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        for(char b : buf.toCharArray()){
            sb.append((int)b);
            sb.append("_");
        }
        return sb.toString();
    }

    private boolean EqualsUTF8(String a, String b) throws UnsupportedEncodingException {
        byte[] ba = a.getBytes("UTF-8");
        byte[] bb = b.getBytes("UTF-8");
        if(ba.length != bb.length) return false;
        for(int i = 0; i < ba.length; ++i){
            if(ba[i] != bb[i]) return false;
        }
        return true;
    }

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        String url = new String(value.getValue("data".getBytes(), "url".getBytes()), "UTF-8");
        String json = new String(value.getValue("data".getBytes(), "data".getBytes()), "UTF-8");

        JSONObject jsonObject;
        try{
            jsonObject = (JSONObject) JSONValue.parse(json);
        } catch (Exception e){
            return;
        }
        if(jsonObject == null) return;

        for(Map.Entry<String, Object> e : jsonObject.entrySet()){
            if(!encode(e.getKey()).equals(encode("相关词条"))) continue;
            Object obj = e.getValue();
            if(obj instanceof String){
                context.write(new Text((String)obj), new Text("1"));
            } else if(obj instanceof JSONArray){
                JSONArray array = (JSONArray) obj;
                for(int i = 0; i < array.size(); ++i){
                    if(array.get(i) instanceof String){
                        context.write(new Text((String)array.get(i)), new Text("1"));
                    }
                }
            }
        }
    }
}
