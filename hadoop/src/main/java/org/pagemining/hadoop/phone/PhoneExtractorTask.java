package org.pagemining.hadoop.phone;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pagemining.hadoop.Constant;

import java.io.IOException;
import java.util.*;

public class PhoneExtractorTask {
    private static int rowKeyLength = 48;

    public static class MapTask extends TableMapper<Text, Text> {

        private List<String> extractPhone(Object obj){
            List<String> ret = new ArrayList<String>();
            if(obj instanceof JSONObject){
                JSONObject jsonObj = (JSONObject) obj;
                for(Map.Entry<String, Object> e : jsonObj.entrySet()){
                    if(!e.getKey().equals("电话号码") && !e.getKey().equals("联系电话")) continue;
                    Object v = e.getValue();
                    if(v instanceof String){
                        ret.add((String) v);
                    } else if(v instanceof JSONObject){
                        List<String> tmp = extractPhone(v);
                        for(String buf : tmp){
                            ret.add(buf);
                        }
                    }
                }
            }
            return ret;
        }

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String url = new String(value.getValue("data".getBytes(), "url".getBytes()), "UTF-8");
            String json = new String(value.getValue("data".getBytes(), "data".getBytes()), "UTF-8");

            JSONObject jsonObject;
            try{
                jsonObject = (JSONObject) JSONValue.parse(json);
            } catch (Exception e){
                return;
            }
            if(jsonObject == null) return;

            List<String> phones = extractPhone(jsonObject);
            for(String phone : phones){
                context.write(new Text(url), new Text(phone));
            }
        }
    }

    public static class ReduceTask extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, java.lang.InterruptedException{
            for(Text value : values){
                context.write(key, value);
            }
        }
    }

    public static void Run(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneExtractorTask.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                Constant.INFO_HBASE_TABLE_NAME,
                scan,
                MapTask.class,
                Text.class,
                Text.class,
                job);
        job.setReducerClass(ReduceTask.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
