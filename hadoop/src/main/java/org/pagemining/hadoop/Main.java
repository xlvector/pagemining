package org.pagemining.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.dict.DictExtractor;
import org.pagemining.hadoop.dict.DictExtractorMapper;
import org.pagemining.hadoop.dict.DictExtractorReducer;
import org.pagemining.hadoop.infoextract.HBaseUtil;
import org.pagemining.hadoop.infoextract.XPathConfigReader;
import org.pagemining.hadoop.infoextract.XPathExtractorMapper;
import org.pagemining.hadoop.infoextract.XPathExtractorReducer;
import org.pagemining.hadoop.linkbase.*;
import org.slf4j.Logger;

public class Main {
    public static void main(String [] args) throws Exception{
        JobConf conf = new JobConf();
        conf.setJobName("pagemining");
        conf.setJarByClass(Main.class);
        String method = args[0];

        if (method.equals("hbase-scan")){
            HTable table = new HTable(conf, args[1]);
            HBaseUtil.scan(table, 10);
            table.close();
        } else if(method.equals("dict-extract")){
            DictExtractor extractor = new DictExtractor();
            extractor.Run(args[1]);
        } else{

            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);


            if(method.equals("stat")) {
                conf.setMapperClass(LinkStatMapper.class);
                conf.setReducerClass(LinkStatReducer.class);
            }
            else if(method.equals("hive-link")) {
                conf.setMapperClass(HiveLinkStatMapper.class);
                conf.setReducerClass(HiveLinkStatReducer.class);
            }
            else if(method.equals("info-extract")) {
                String[] cf = {"data"};
                HBaseUtil.createTableIfNotExist(Constant.INFO_HBASE_TABLE_NAME, conf, cf);

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
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setNumReduceTasks(8);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(args[2]), true);
            FileInputFormat.addInputPaths(conf, args[1]);
            FileOutputFormat.setOutputPath(conf, new Path(args[2]));

            JobClient.runJob(conf);
        }
    }
}
