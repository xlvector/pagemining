package org.pagemining.hadoop.knowledgegraph;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KnowledgeGraphTask {
}