package org.pagemining.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.infoextract.XPathConfigReader;
import org.pagemining.hadoop.infoextract.XPathExtractorMapper;
import org.pagemining.hadoop.infoextract.XPathExtractorReducer;
import org.pagemining.hadoop.linkbase.LinkBaseMapper;
import org.pagemining.hadoop.linkbase.LinkBaseReducer;
import org.pagemining.hadoop.linkbase.LinkStatMapper;
import org.pagemining.hadoop.linkbase.LinkStatReducer;

public class Main {
    public static void main(String [] args) throws Exception{
        JobConf conf = new JobConf();
        conf.setJobName("pagemining");

        // This line specifies the jar Hadoop should use to run the mapper and
        // reducer by telling it a class that’s inside it
        conf.setJarByClass(Main.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        String method = args[0];
        if(method.equals("stat")) {
            conf.setMapperClass(LinkStatMapper.class);
            conf.setReducerClass(LinkStatReducer.class);
        }
        else if(method.equals("info-extract")) {
            String xpathConfig = XPathConfigReader.readConfig(args[3]);
            conf.set("xpath.config", xpathConfig);
            conf.setMapperClass(XPathExtractorMapper.class);
            conf.setReducerClass(XPathExtractorReducer.class);
        }
        else if(method.equals("link-extract")) {
            conf.setMapperClass(LinkBaseMapper.class);
            conf.setReducerClass(LinkBaseReducer.class);
        }
        else {
            System.out.println("Does not support METHOD " + method);
            System.exit(1);
        }
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");

        // KeyValueTextInputFormat treats each line as an input record,
        // and splits the line by the tab character to separate it into key and value
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setNumReduceTasks(8);
        FileInputFormat.addInputPaths(conf, args[1]);
        FileOutputFormat.setOutputPath(conf, new Path(args[2] + "/" + String.valueOf(System.currentTimeMillis())));

        JobClient.runJob(conf);
    }
}
