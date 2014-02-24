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
import java.util.ArrayList;
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

    private List<String> extractPhone(Object obj){
        List<String> ret = new ArrayList<String>();
        if(obj instanceof JSONObject){
            JSONObject jsonObj = (JSONObject) obj;
            for(Map.Entry<String, Object> e : jsonObj.entrySet()){
                if(!e.getKey().equals("电话号码") && !e.getKey().equals("联系电话")) continue;
                Object v = e.getValue();
                if(v instanceof String){
                    ret.add((String) v);
                } else if(v instanceof JSONObject){
                    List<String> tmp = extractPhone(v);
                    for(String buf : tmp){
                        ret.add(buf);
                    }
                }
            }
        }
        return ret;
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

        List<String> phones = extractPhone(jsonObject);
        for(String phone : phones){
            context.write(new Text(url), new Text(phone));
        }
    }
}
