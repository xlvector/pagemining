package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class LinkBaseReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        long maxTimeStamp = 0L;
        while (values.hasNext()){
            Long tm = Long.parseLong(values.next().toString());
            if(maxTimeStamp < tm){
                maxTimeStamp = tm;
            }
        }
        collector.collect(new Text(key), new Text(String.valueOf(maxTimeStamp)));
    }
}
