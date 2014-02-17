package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class HiveLinkStatReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        while (values.hasNext()){
            String [] tks = key.toString().split("\t");
            if(tks.length != 2) continue;
            String link = tks[0];
            String domain = tks[1];
            collector.collect(new Text(link + "\t" + values.next().toString()), new Text(domain));
        }
    }
}
