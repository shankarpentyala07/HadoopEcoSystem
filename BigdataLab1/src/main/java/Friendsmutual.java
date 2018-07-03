import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;



public class Friendsmutual {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{


        private Text outputkey = new Text();
        private Text outputvalue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
           String key1 = itr.nextToken();
            String Value1 = " ";

            while (itr.hasMoreTokens())
            {
                String x = itr.nextToken();
                Value1 += x + " ";
            }
            StringTokenizer Value2 = new StringTokenizer(Value1);

            while (Value2.hasMoreTokens())
            {
                String temp = Value2.nextToken();
                 if (key1.compareTo(temp) < 0)
                 {
                     outputkey.set(key1 + " " + temp);
                 }
                 else
                 {
                     outputkey.set(temp + " " + key1);
                 }

                outputvalue.set(Value1);
               context.write(outputkey,outputvalue);
            }



        }
    }

    public static class mutualfriendsreduce
            extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String friendsarray[] = new String[2];
            int index = 0;
            for(Text value : values)
                friendsarray[index++] = value.toString();
           String temp1 = findmutualfriends(friendsarray[0],friendsarray[1]);
            context.write(key,new Text(temp1));
        }

        static String findmutualfriends(String set1,String set2)
        {
            if (set1 == null || set2 == null)
                 return null;
            String[] a1 = set1.split(" ");
            String[] a2 = set2.split("");
            HashSet hs1 = new HashSet();
            HashSet hs2 = new HashSet();


            for(int i =0;i<a1.length;i++)
                hs1.add(a1[i]);

            for(int i =0;i<a2.length;i++)
                hs2.add(a2[i]);

            hs1.retainAll(hs2);

            return hs1.toString();



        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friendsmutual");
        job.setJarByClass(Friendsmutual.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(mutualfriendsreduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("Data/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("Data/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}