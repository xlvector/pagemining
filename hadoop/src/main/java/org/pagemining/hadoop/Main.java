package org.pagemining.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.dict.DictExtractor;
import org.pagemining.hadoop.dict.DictExtractorMapper;
import org.pagemining.hadoop.dict.DictExtractorReducer;
import org.pagemining.hadoop.infoextract.HBaseUtil;
import org.pagemining.hadoop.infoextract.XPathConfigReader;
import org.pagemining.hadoop.infoextract.XPathExtractorMapper;
import org.pagemining.hadoop.infoextract.XPathExtractorReducer;
import org.pagemining.hadoop.linkbase.*;
import org.pagemining.hadoop.phone.PhoneExtractorMapper;
import org.pagemining.hadoop.phone.PhoneExtractorReducer;
import org.slf4j.Logger;

public class Main {
    public static void main(String [] args) throws Exception{
        Options options = new Options();
        CommandLineParser parser = new BasicParser();

        options.addOption("job", true, "job name");
        options.addOption("input", true, "input path");
        options.addOption("output", true, "output path");
        options.addOption("config", true, "config path");

        CommandLine cmd = parser.parse(options, args);

        JobConf conf = new JobConf();
        String method = cmd.getOptionValue("job");
        conf.setJobName(method);
        conf.setJarByClass(Main.class);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(cmd.getOptionValue("output")), true);

        if (method.equals("hbase-scan")){
            HTable table = new HTable(conf, cmd.getOptionValue("input"));
            HBaseUtil.scan(table, 10);
            table.close();
        } else if(method.equals("dict-extract")){
            DictExtractor extractor = new DictExtractor();
            extractor.Run(cmd.getOptionValue("output"));
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
                System.out.println(cmd.getOptionValue("config"));
                String xpathConfig = XPathConfigReader.readConfig(cmd.getOptionValue("config"));
                conf.set("xpath.config", xpathConfig);
                conf.setMapperClass(XPathExtractorMapper.class);
                conf.setReducerClass(XPathExtractorReducer.class);
            }
            else if(method.equals("link-extract")) {
                conf.setMapperClass(LinkBaseMapper.class);
                conf.setReducerClass(LinkBaseReducer.class);
            }
            else if(method.equals("domain")) {
                conf.setMapperClass(DomainGroupMapper.class);
                conf.setReducerClass(DomainGroupReducer.class);
            }
            else if(method.equals("phone-extract")) {
                conf.setMapperClass(PhoneExtractorMapper.class);
                conf.setReducerClass(PhoneExtractorReducer.class);
            }
            else {
                System.out.println("Does not support METHOD " + method);
                System.exit(1);
            }
            conf.set("mapreduce.map.memory.mb", "1024");
            conf.set("mapreduce.reduce.memory.mb", "2048");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            // KeyValueTextInputFormat treats each line as an input record,
            // and splits the line by the tab character to separate it into key and value
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setNumReduceTasks(8);

            FileInputFormat.addInputPaths(conf, cmd.getOptionValue("input"));

            FileOutputFormat.setOutputPath(conf, new Path(cmd.getOptionValue("output")));

            JobClient.runJob(conf);
        }
    }
}
