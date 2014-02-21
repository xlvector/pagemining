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
import java.util.List;
import java.util.Map;

public class DictExtractorMapper extends TableMapper<Text, Text> {
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        String url = new String(value.getValue("data".getBytes(), "url".getBytes()), "UTF-8");
        String json = new String(value.getValue("data".getBytes(), "data".getBytes()), "UTF-8");

        JSONObject jsonObject = null;
        try{
            jsonObject = (JSONObject) JSONValue.parse(json);
        } catch (Exception e){
            return;
        }
        if(jsonObject == null) return;
        /*
        if(jsonObject.containsKey("相关词条")){
            Object obj = jsonObject.get("相关词条");
            if(obj instanceof String){
                context.write(new Text((String)obj), new Text("1"));
            } else if(obj instanceof JSONArray){
                JSONArray array = (JSONArray) obj;
                for(int i = 0; i < array.size(); ++i){
                    context.write(new Text((String)array.get(i)), new Text("1"));
                }
            }
        }
        */
        for(Map.Entry<String, Object> e : jsonObject.entrySet()){
            context.write(new Text(e.getKey()), new Text("1"));
        }
    }
}
