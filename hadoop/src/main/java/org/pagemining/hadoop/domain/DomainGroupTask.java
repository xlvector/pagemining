package org.pagemining.hadoop.domain;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DomainGroupTask {
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] tks = value.toString().split("\t");
            if(tks.length != 3) return;

            String timestamp = tks[0];
            String url = tks[1];
            String html = tks[2];

            Document doc = Jsoup.parse(html);
            Elements scripts = doc.select("script");
            for(Element e : scripts){
                if(e.html().length() > 256){
                    e.remove();
                }
            }
            Elements styles = doc.select("style");
            for(Element e : styles){
                e.remove();
            }
            StringBuilder sb = new StringBuilder();
            sb.append(url);
            sb.append("\t");
            sb.append(DomainUtil.cleanHtml(doc.html()));
            context.write(new LongWritable(Long.parseLong(timestamp)), new Text(sb.toString()));
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        private MultipleOutputs<LongWritable, Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            super.setup(context);
            mos = new MultipleOutputs<LongWritable, Text>(context);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                String[] tks = value.toString().split("\t");
                String domain = DomainUtil.getDomain(tks[0]);
                String topDomain = DomainUtil.getTopDomain(domain);

                String fileName = topDomain;
                fileName = fileName.replace(".", "_");
                mos.write(key, value, fileName);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            mos.close();
        }
    }

    public static void Run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec",Lz4Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setJarByClass(DomainGroupTask.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}
