package org.pagemining.hadoop.infoextract;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

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
        admin.createTable(descriptor);
    }

    public static void insert(String colFamily, HTable table, String key, Map<String, String> columns) throws IOException {
        Put put = new Put(key.getBytes());
        for(Map.Entry<String, String> col : columns.entrySet()){
            put.add(colFamily.getBytes(), col.getKey().getBytes("UTF-8"), col.getValue().getBytes("UTF-8"));
        }
        table.put(put);
    }

    public static void scan(HTable table, int limit) {
        try {
            ResultScanner rs = table.getScanner(new Scan());
            int n = 0;
            for (Result r : rs) {
                if(++n > limit) break;
                System.out.println(new String(new String(r.getRow()).getBytes("UTF-8")));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(new String(new String(keyValue.getFamily()).getBytes("UTF-8")));
                    System.out.println(new String(new String(keyValue.getValue()).getBytes("UTF-8")));
                }
                System.out.println("========");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
