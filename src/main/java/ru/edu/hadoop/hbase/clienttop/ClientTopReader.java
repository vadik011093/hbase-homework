package ru.edu.hadoop.hbase.clienttop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.max;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class ClientTopReader {

    private static final byte[] DATA_COLUMNFAMILY = toBytes("data");
    private static final byte[] URL_QUALIFIER = toBytes("url");
    private static final byte[] TIME_QUALIFIER = toBytes("time");

    public static void main(String[] args) throws IOException {

        if (args.length < 2)
            throw new IllegalArgumentException("You should fill date from and date to");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date from = null;
        Date to = null;
        try {
            from = dateFormat.parse(args[0]);
            to = dateFormat.parse(args[1]);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Date must be formatted to yyyy-MM-dd", e);
        }

        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("domain");
        HTable table = new HTable(conf, tableName);


        Scan scan = new Scan(toBytes(dateFormat.format(from)), toBytes(dateFormat.format(DateUtil.addDays(to, 1))));
        scan.addColumn(DATA_COLUMNFAMILY, URL_QUALIFIER);
        scan.addColumn(DATA_COLUMNFAMILY, TIME_QUALIFIER);
        ResultScanner scanner = table.getScanner(scan);

        Result res = null;

        Map<URL, DomainStatistic> domainStatistics = new HashMap<>();

        double total = 0;

        try {
            while ((res = scanner.next()) != null) {
                URL url = new URL(Bytes.toString(res.getValue(DATA_COLUMNFAMILY, URL_QUALIFIER)));
                int diffTime = Integer.valueOf(Bytes.toString(res.getValue(DATA_COLUMNFAMILY, TIME_QUALIFIER)));
                total += diffTime;
                DomainStatistic domainStatistic = domainStatistics.get(url);
                if (domainStatistic == null) {
                    domainStatistic = new DomainStatistic(url, 0);
                    domainStatistics.put(url, domainStatistic);
                }
                domainStatistic.addDiffTime(diffTime);
            }
        } finally {
            scanner.close();
        }

        for (int i = 0; i < 10; i++) {
            DomainStatistic max = max(domainStatistics.values());
            System.out.printf("%s %f%n", max.url().toString(), max.diffTime() / total);
            domainStatistics.remove(max.url());
        }
    }

    private static class DomainStatistic implements Comparable<DomainStatistic> {

        private final URL url;
        private int diffTime;

        private DomainStatistic(URL url, int diffTime) {
            this.url = url;
            this.diffTime = diffTime;
        }

        private URL url() {
            return url;
        }

        private int diffTime() {
            return diffTime;
        }

        private void addDiffTime(int diffTime) {
            this.diffTime += diffTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DomainStatistic that = (DomainStatistic) o;
            return url.equals(that.url);
        }

        @Override
        public int hashCode() {
            return url.hashCode();
        }

        @Override
        public int compareTo(DomainStatistic o) {
            return Integer.compare(diffTime(), o.diffTime());
        }

        @Override
        public String toString() {
            return "DomainStatistic{" +
                    "url=" + url +
                    ", diffTime=" + diffTime +
                    '}';
        }
    }

    private static class DateUtil {
        private static Date addDays(Date date, int days) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, days);
            return cal.getTime();
        }
    }

}