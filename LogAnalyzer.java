import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop MapReduce program for analyzing web server logs.
 * Log format example:
 *   IP - - [timestamp] "METHOD URL HTTP/1.1" status bytes
 *
 * Usage:
 *   hadoop jar LogAnalyzer.jar LogAnalyzer <input> <output> <analysis-type>
 *
 * Analysis types:
 *   status, ip, url, hour, bytes
 */
public class LogAnalyzer {

    // Regular expression for Apache-style logs
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+) \\S+ \\S+ \\[(.+?)\\] \\\"(\\S+) (\\S+) .+?\\\" (\\d+) (\\d+).*"
    );

    /**
     * Mapper class for processing each log line and emitting key-value pairs
     * depending on the analysis type (status, ip, url, hour, bytes).
     */
    public static class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            Matcher matcher = LOG_PATTERN.matcher(line);

            if (matcher.matches()) {
                String ip = matcher.group(1);
                String timestamp = matcher.group(2);
                String method = matcher.group(3);
                String url = matcher.group(4);
                int status = Integer.parseInt(matcher.group(5));
                int bytes = Integer.parseInt(matcher.group(6));

                // Get analysis type from configuration
                String analysisType = context.getConfiguration().get("analysis.type", "status");

                switch (analysisType) {
                    case "status":
                        outputKey.set("Status_" + status);
                        context.write(outputKey, outputValue);
                        break;

                    case "ip":
                        outputKey.set(ip);
                        context.write(outputKey, outputValue);
                        break;

                    case "url":
                        outputKey.set(url);
                        context.write(outputKey, outputValue);
                        break;

                    case "hour":
                        String hour = extractHour(timestamp);
                        outputKey.set("Hour_" + hour);
                        context.write(outputKey, outputValue);
                        break;

                    case "bytes":
                        outputKey.set("TotalBytes");
                        outputValue.set(bytes);
                        context.write(outputKey, outputValue);
                        break;

                    default:
                        // Unknown analysis type — do nothing
                        break;
                }
            }
        }

        /**
         * Extracts the hour (HH) from a timestamp like: 01/Jul/2023:12:34:56 +0000
         */
        private String extractHour(String timestamp) {
            try {
                SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
                Date date = format.parse(timestamp.split(" ")[0]);
                SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
                return hourFormat.format(date);
            } catch (ParseException e) {
                return "Unknown";
            }
        }
    }

    /**
     * Reducer class for aggregating counts or summing bytes.
     */
    public static class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Main driver method for the LogAnalyzer MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: LogAnalyzer <input> <output> <analysis-type>");
            System.err.println("Analysis types: status, ip, url, hour, bytes");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("analysis.type", args[2]);

        Job job = Job.getInstance(conf, "Log Analyzer - " + args[2]);
        job.setJarByClass(LogAnalyzer.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
