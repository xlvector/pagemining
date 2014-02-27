package org.pagemining.hadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.pagemining.hadoop.phone.PhoneExtractorTask;
import org.pagemining.hadoop.domain.DomainGroupTask;
import org.pagemining.hadoop.infoextract.HBaseUtil;
import org.pagemining.hadoop.infoextract.XPathExtractorTask;
import org.pagemining.hadoop.linkbase.*;

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
        } else if(method.equals("phone-extract")){
            PhoneExtractorTask.Run(cmd.getOptionValue("input"), cmd.getOptionValue("output"));
        } else if(method.equals("domain")){
            DomainGroupTask.Run(cmd.getOptionValue("input"), cmd.getOptionValue("output"));
        } else if(method.equals("info-extract")) {
            XPathExtractorTask.Run(cmd.getOptionValue("input"), cmd.getOptionValue("output"), cmd.getOptionValue("config"));
        } else if(method.equals("stat")){
            LinkStatTask.Run(cmd.getOptionValue("input"), cmd.getOptionValue("output"));
        }
        else{

            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            if(method.equals("hive-link")) {
                conf.setMapperClass(HiveLinkStatMapper.class);
                conf.setReducerClass(HiveLinkStatReducer.class);
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
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);
            conf.setBoolean("mapred.output.compress", true);
            conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
            //conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setNumReduceTasks(8);

            FileInputFormat.addInputPaths(conf, cmd.getOptionValue("input"));

            FileOutputFormat.setOutputPath(conf, new Path(cmd.getOptionValue("output")));

            JobClient.runJob(conf);
        }
    }
}
