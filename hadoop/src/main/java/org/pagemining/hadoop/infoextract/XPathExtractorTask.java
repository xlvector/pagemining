package org.pagemining.hadoop.infoextract;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.CharSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.pagemining.extractor.xpath.XPathConfig;
import org.pagemining.extractor.xpath.XPathExtractor;
import org.pagemining.hadoop.Constant;
import org.pagemining.hadoop.domain.DomainUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class XPathExtractorTask {
    private static int rowKeyLength = 48;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private XPathExtractor extractor = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            extractor = new XPathExtractor();
            String configStr = context.getConfiguration().get("xpath.config");
            JSONArray jsonArray = (JSONArray) JSONValue.parse(configStr);
            for(int i = 0; i < jsonArray.size(); ++i){
                XPathConfig config = new XPathConfig();
                config.fromString(((JSONObject)jsonArray.get(i)).toString());
                extractor.AddConfig(config);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] tks = value.toString().split("\t");
            if(tks.length != 3) return;

            String timestamp = tks[0];
            String url = tks[1];
            String html = tks[2];

            JSONObject jsonDoc = extractor.extract(url, html);
            if(jsonDoc != null){
                jsonDoc.put("_crawled_at", timestamp);
                context.write(new Text(url), new Text(jsonDoc.toString()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        HTable table = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            try {
                table = new HTable(context.getConfiguration(), Constant.INFO_HBASE_TABLE_NAME);
            } catch (IOException e) {
                return;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                String rowkey = HBaseUtil.getRowKey(key.toString(), rowKeyLength);
                if(rowkey == null) return;
                for(Text value : values){
                    java.util.Map<String, String> column = new HashMap<String, String>();
                    context.write(key, value);
                    column.put("url", key.toString());
                    column.put("data", value.toString());
                    HBaseUtil.insert("data", table, rowkey, column);
                    break;
                }
            } catch (IOException e){
                return;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            table.close();
        }
    }

    public static void Run(String input, String output, String config) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        String[] cf = {"data"};
        HBaseUtil.createTableIfNotExist(Constant.INFO_HBASE_TABLE_NAME, conf, cf, rowKeyLength);
        String xpathConfig = XPathConfigReader.readConfig(config);
        conf.set("xpath.config", xpathConfig);

        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setJarByClass(XPathExtractorTask.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(8);
        job.waitForCompletion(true);
    }
}
