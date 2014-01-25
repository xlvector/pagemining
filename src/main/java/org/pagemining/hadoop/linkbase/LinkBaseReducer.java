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
        int n = 0;
        while (values.hasNext()){
            n += 1;
            LinkInfo linkInfo = new LinkInfo();
            linkInfo.fromString(values.next().toString());
            if(linkInfo.getUpdatedAt() > 0) {
                return;
            }
        }
        collector.collect(new Text(key), new Text(String.valueOf(n)));
    }
}
