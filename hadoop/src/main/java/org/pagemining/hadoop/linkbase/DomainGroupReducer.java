package org.pagemining.hadoop.linkbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class DomainGroupReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Configuration conf = null;

    @Override
    public void configure(JobConf conf){
        this.conf = conf;
    }

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        String output = "crawler/domains/" + key.toString() + "/data.seq";
        Path path = new Path(output);
        SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD, new GzipCodec()),
                SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class));

        while (values.hasNext()){
            Text value = values.next();
            writer.append(new Text(String.valueOf(System.currentTimeMillis())), value);
        }
        writer.close();
    }
}
