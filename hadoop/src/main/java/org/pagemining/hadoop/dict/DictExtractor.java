package org.pagemining.hadoop.dict;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.pagemining.hadoop.Constant;

import java.io.IOException;

public class DictExtractor {
    public void Run(String path) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJarByClass(this.getClass());
        job.setJobName(this.getClass().getName());

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                Constant.INFO_HBASE_TABLE_NAME,
                scan,
                DictExtractorMapper.class,
                Text.class,
                Text.class,
                job);
        job.setReducerClass(DictExtractorReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(path));

        job.waitForCompletion(true);
    }
}
