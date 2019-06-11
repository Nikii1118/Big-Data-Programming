package com.wordcount;

/*import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
*/
import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



    public class WordCount {

        public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] indicesAndValue = line.split(",");
                Text outputKey = new Text();
                Text outputValue = new Text();
                if (indicesAndValue[0].equals("A")) {
                    outputKey.set(indicesAndValue[2]);
                    outputValue.set("A," + indicesAndValue[1] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                } else {
                    outputKey.set(indicesAndValue[1]);
                    outputValue.set("B," + indicesAndValue[2] + "," + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }


        public static class Reduce extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String[] value;
                ArrayList<Entry<Integer, Float>> listA = new ArrayList<Entry<Integer, Float>>();
                ArrayList<Entry<Integer, Float>> listB = new ArrayList<Entry<Integer, Float>>();
                for (Text val : values) {
                    value = val.toString().split(",");
                    if (value[0].equals("A")) {
                        listA.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                    } else {
                        listB.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                    }
                }
                String i;
                float a_ij;
                String k;
                float b_jk;
                Text outputValue = new Text();
                for (Entry<Integer, Float> a : listA) {
                    i = Integer.toString(a.getKey());
                    a_ij = a.getValue();
                    for (Entry<Integer, Float> b : listB) {
                        k = Integer.toString(b.getKey());
                        b_jk = b.getValue();
                        outputValue.set(i + "," + k + "," + Float.toString(a_ij*b_jk));
                        context.write(null, outputValue);
                    }
                }
            }
        }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "WordCount");
            job.setJarByClass(WordCount.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path("./Input1.text"));
            FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/output5"));

            job.waitForCompletion(true);
        }
    }


         /* public static void main(String[] args) throws Exception {
              if (args.length != 2) {
                  System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
                  System.exit(2);
              }
              Configuration conf = new Configuration();
              // M is an m-by-n matrix; N is an n-by-p matrix.
              conf.set("m", "1000");
              conf.set("n", "100");
              conf.set("p", "1000");
              @SuppressWarnings("deprecation")
              Job job = new Job(conf, "MatrixMultiply");
              job.setJarByClass(MatrixMultiply.class);
              job.setOutputKeyClass(Text.class);
              job.setOutputValueClass(Text.class);

              job.setMapperClass(Map.class);
              job.setReducerClass(Reduce.class);

              job.setInputFormatClass(TextInputFormat.class);
              job.setOutputFormatClass(TextOutputFormat.class);



              FileInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/input.txt"));
              FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/output5"));

              job.waitForCompletion(true);
          }
      }


public class Map
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        // (M, i, j, Mij);
        String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (indicesAndValue[0].equals("M")) {
            for (int k = 0; k < p; k++) {
                outputKey.set(indicesAndValue[1] + "," + k);
                // outputKey.set(i,k);
                outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2]
                        + "," + indicesAndValue[3]);
                // outputValue.set(M,j,Mij);
                context.write(outputKey, outputValue);
            }
        } else {
            // (N, j, k, Njk);
            for (int i = 0; i < m; i++) {
                outputKey.set(i + "," + indicesAndValue[2]);
                outputValue.set("N," + indicesAndValue[1] + ","
                        + indicesAndValue[3]);
                context.write(outputKey, outputValue);
            }
        }
    }
}*/
 /* public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }*/
/* public class Reduce
         extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
     @Override
     public void reduce(Text key, Iterable<Text> values, Context context)
             throws IOException, InterruptedException {
         String[] value;
         //key=(i,k),
         //Values = [(M/N,j,V/W),..]
         HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
         HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
         for (Text val : values) {
             value = val.toString().split(",");
             if (value[0].equals("M")) {
                 hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
             } else {
                 hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
             }
         }
         int n = Integer.parseInt(context.getConfiguration().get("n"));
         float result = 0.0f;
         float m_ij;
         float n_jk;
         for (int j = 0; j < n; j++) {
             m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
             n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
             result += m_ij * n_jk;
         }
         if (result != 0.0f) {
             context.write(null,
                     new Text(key.toString() + "," + Float.toString(result)));
         }
     }
 }*/
   /* public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private String strNumber = "";
        private int number = 0;
        int sum = 0;
        int count = 0;
        int count1 = 0;

        int sum1 = 0;

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                sum += val.get();
            }
            strNumber = key.toString();
            number = Integer.parseInt(strNumber);*/

           /* if (number % 2 == 0) {
                count = count + 1;
            }
            else {
                result.set(count1 = count1 + 1);
            }*/

       /*     result.set((number %2 == 0)? 1: 0);
            context.write(key, result);
        }

    }
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/input.txt"));
            FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/output2"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

*/