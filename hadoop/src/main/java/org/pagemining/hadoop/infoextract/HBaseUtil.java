package org.pagemining.hadoop.infoextract;


import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.pagemining.hadoop.domain.DomainUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class HBaseUtil {

    public static void createTableIfNotExist(String tableName, Configuration conf, String [] familyNames, int rowKeyLength) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            return;
        }
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        for(String familyName : familyNames){
            descriptor.addFamily(new HColumnDescriptor(familyName));
        }
        admin.createTable(descriptor, getStringSplits(rowKeyLength));
    }

    public static byte[][] getStringSplits(int length) {
        byte[][] splits = new byte[36][];
        int n = 0;
        for(char ch = '0'; ch <= '9'; ch++){
            StringBuilder sb = new StringBuilder();
            sb.append(ch);
            while (sb.length() < length){
                sb.append('0');
            }
            splits[n] = sb.toString().getBytes();
            n += 1;
        }
        for(char ch = 'a'; ch <= 'z'; ch++){
            StringBuilder sb = new StringBuilder();
            sb.append(ch);
            while (sb.length() < length){
                sb.append('0');
            }
            splits[n] = sb.toString().getBytes();
            n += 1;
        }
        return splits;
    }

    public static void insert(String colFamily, HTable table, String key, Map<String, String> columns) throws IOException {
        Put put = new Put(key.getBytes());
        for(Map.Entry<String, String> col : columns.entrySet()){
            put.add(colFamily.getBytes(), col.getKey().getBytes("UTF-8"), col.getValue().getBytes("UTF-8"));
        }
        table.put(put);
    }

    public static String getStartRowKey(String url, int length){
        String domain = DomainUtil.getDomain(url);
        StringBuilder sb = new StringBuilder();
        sb.append(domain.replace(".", "_"));
        sb.append("_");
        while (sb.length() < length){
            sb.append("0");
        }
        return sb.toString().substring(0, length);
    }

    public static String getEndRowKey(String url, int length){
        String domain = DomainUtil.getDomain(url);
        StringBuilder sb = new StringBuilder();
        sb.append(domain.replace(".", "_"));
        sb.append("_");
        while (sb.length() < length){
            sb.append("z");
        }
        return sb.toString().substring(0, length);
    }

    public static String getRowKey(String url, int length){
        String domain = DomainUtil.getDomain(url);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.reset();
            md.update(url.getBytes("UTF-8"));
            String en = new String(Hex.encodeHex(md.digest()));
            StringBuilder sb = new StringBuilder();
            sb.append(domain.replace(".", "_"));
            sb.append("_");
            sb.append(en);
            while (sb.length() < length){
                sb.append("z");
            }
            return sb.toString().substring(0, length);
        } catch (NoSuchAlgorithmException e) {
            return null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    public static void scan(HTable table, int limit) {
        try {
            ResultScanner rs = table.getScanner(new Scan());
            int n = 0;
            for (Result r : rs) {
                if(++n > limit) break;
                System.out.println(new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(new String(keyValue.getFamily()));
                    System.out.println(new String(keyValue.getValue()));
                    try{
                        JSONObject jsonObject = (JSONObject)JSONValue.parse(keyValue.getValue());
                        System.out.println(jsonObject.toString());
                    } catch (Exception e){
                        continue;
                    }
                }
                System.out.println("========");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
