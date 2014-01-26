package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LinkBaseReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        Set<String> sources = new HashSet<String>();
        boolean hasDownload = false;
        while (values.hasNext()){
            LinkInfo linkInfo = new LinkInfo();
            linkInfo.fromString(values.next().toString());
            sources.add(linkInfo.getSrcLink());
            if(linkInfo.getUpdatedAt() > 0) {
                hasDownload = true;
            }
        }
        collector.collect(new Text(key), new Text(String.valueOf(sources.size()) + "\t" + String.valueOf(hasDownload)));
    }
}
