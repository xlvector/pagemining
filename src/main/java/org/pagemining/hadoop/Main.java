package org.pagemining.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.linkbase.LinkBaseMapper;
import org.pagemining.hadoop.linkbase.LinkBaseReducer;

public class Main {
    public static void main(String [] args) throws Exception{
        JobConf conf = new JobConf();
        conf.setJobName("pagemining");

        // This line specifies the jar Hadoop should use to run the mapper and
        // reducer by telling it a class that’s inside it
        conf.setJarByClass(Main.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(LinkBaseMapper.class);
        conf.setReducerClass(LinkBaseReducer.class);

        // KeyValueTextInputFormat treats each line as an input record,
        // and splits the line by the tab character to separate it into key and value
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
