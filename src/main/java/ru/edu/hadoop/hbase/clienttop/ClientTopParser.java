package ru.edu.hadoop.hbase.clienttop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class ClientTopParser {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ClientTopParser.class);
        job.setMapperClass(HBaseMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(8);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(NullOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HBaseMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private HTable table;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split("\t");
            String userId = split[0];
            Date date = Date.from(Instant.ofEpochSecond(Long.valueOf(split[1])));
            URL url = new URL(split[2]);
            int diffTime = Integer.valueOf(split[3].split(":")[1]);

            table.put(createPut(userId, date, url, diffTime));
        }

        private Put createPut(String userId, Date date, URL url, int diffTime) {
            Put put = new Put(Bytes.toBytes(String.format("%s %s", new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(date), userId)));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("url"),
                    Bytes.toBytes(url.toString()));
            put.add(Bytes.toBytes("data"), Bytes.toBytes("time"),
                    Bytes.toBytes(Integer.toString(diffTime)));
            return put;
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                Configuration conf = HBaseConfiguration.create();
                TableName tableName = TableName.valueOf("domain");
                table = new HTable(conf, tableName);
            } catch (IOException e) {
                throw new RuntimeException("Failed HTable construction", e);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            table.close();
        }

    }

}