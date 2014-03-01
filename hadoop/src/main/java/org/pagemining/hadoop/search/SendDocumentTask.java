package org.pagemining.hadoop.search;

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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SendDocumentTask {
    private static int rowKeyLength = 48;

    public static class MapTask extends TableMapper<Text, Text> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String link = new String(value.getValue("data".getBytes(), "url".getBytes()), "UTF-8");
            String json = new String(value.getValue("data".getBytes(), "data".getBytes()), "UTF-8");

            URL url = new URL("http://10.180.60.12/documents/" + row.toString());
            JSONObject jsonObject = (JSONObject)JSONValue.parse(json);
            jsonObject.put("_url", link);
            HttpURLConnection httpURLConnection = null;
            DataOutputStream dataOutputStream = null;
            try {
                httpURLConnection = (HttpURLConnection) url.openConnection();
                httpURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                httpURLConnection.setRequestMethod("PUT");
                httpURLConnection.setDoInput(true);
                httpURLConnection.setDoOutput(true);
                dataOutputStream = new DataOutputStream(httpURLConnection.getOutputStream());
                dataOutputStream.write(jsonObject.toString().getBytes("UTF-8"));
            }catch (Exception e){
                return;
            }finally {
                if (dataOutputStream != null) {
                    try {
                        dataOutputStream.flush();
                        dataOutputStream.close();
                    } catch (IOException exception) {
                        exception.printStackTrace();
                    }
                }
                if (httpURLConnection != null) {
                    httpURLConnection.disconnect();
                }
            }
        }
    }

    public static void Run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SendDocumentTask.class);

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
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}
