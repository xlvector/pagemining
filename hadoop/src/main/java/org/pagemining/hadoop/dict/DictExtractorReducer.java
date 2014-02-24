package org.pagemining.hadoop.dict;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DictExtractorReducer extends Reducer<Text, Text, Text, Text> {
     @Override
     public void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, java.lang.InterruptedException{
         for(Text value : values){
            context.write(key, value);
         }
     }
}
