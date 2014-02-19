package org.pagemining.hadoop.infoextract;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.Map;

public class HBaseUtil {

    public static void createTableIfNotExist(String tableName, Configuration conf, String [] familyNames) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            return;
        }
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        for(String familyName : familyNames){
            descriptor.addFamily(new HColumnDescriptor(familyName));
        }
    }

    public static void insert(String colFamily, HTable table, String key, Map<String, String> columns) throws IOException {
        Put put = new Put(key.getBytes());
        for(Map.Entry<String, String> col : columns.entrySet()){
            put.add(colFamily.getBytes(), col.getKey().getBytes(), col.getValue().getBytes());
        }
        table.put(put);
    }

}
