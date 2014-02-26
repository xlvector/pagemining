package org.pagemining.hadoop.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DomainGroupTask {
    public static class Map extends Mapper<LongWritable, Text, NullWritable, Text>{
        private MultipleOutputs mos;

        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            super.setup(context);
            mos = new MultipleOutputs(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] tks = value.toString().split("\t");
            if(tks.length != 3) return;

            String url = tks[1];
            String domain = DomainUtil.getDomain(url);
            if(domain != null){
                String filename = DomainUtil.getFileNameByDomain(domain);
                mos.write(NullWritable.get(), value, filename + "/");
            }
        }
    }

    public static void Run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setJarByClass(DomainGroupTask.class);
        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
    }
}
