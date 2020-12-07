import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class My_values
{
    public int count_1;
    public int sum_x_1;
    public int sum_y_1;
}

public class kmeans
{

    public static void main(String[] args) throws Exception
    {
        ////loop control
        int count_times_equal=0;
        for(int i=0;i<6;i++)
        {
            Configuration conf=new Configuration();

            Path centroids_file=new Path(args[0]);
            //deal with centroid file
            if(i==0)
            {
                conf.set("centroids",centroids_file.toString());
            }
            else
            {
                conf.set("centroids",args[2]+(i-1)+"/part-r-00000");
            }

            ////compare centroids if didn't change then exit
            Job job=Job.getInstance(conf,"kmeans");
            job.setJarByClass(kmeans.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(my_mapper.class);

            job.setCombinerClass(my_combiner.class);
            job.setReducerClass(my_reducer.class);

            job.setNumReduceTasks(1);

            //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, my_mapper.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]+i+"/"));

            job.waitForCompletion(true);

            String old_cen="";
            Path cen_file_path=new Path(conf.get("centroids"));
            FileSystem my_fs=FileSystem.get(conf);
            BufferedReader my_br=new BufferedReader(new InputStreamReader(my_fs.open(cen_file_path)));

            try
            {
                String line;
                line=my_br.readLine();
                while(line!=null)
                {
                    old_cen+=line;
                    line=my_br.readLine();
                }
            }
            finally
            {
                my_br.close();
            }

            ////compare centroids if didn't change then exit
            String new_cen="";
            Path cen_file_path1=new Path(args[2]+i+"/part-r-00000");
            FileSystem my_fs1=FileSystem.get(conf);
            BufferedReader my_br1=new BufferedReader(new InputStreamReader(my_fs1.open(cen_file_path1)));

            try
            {
                String line;
                line=my_br1.readLine();
                while(line!=null)
                {
                    new_cen+=line;
                    line=my_br1.readLine();
                }
            }
            finally
            {
                my_br1.close();
            }

            if(old_cen.equals(new_cen)==true)
            {
                count_times_equal++;
                if(count_times_equal==2)
                {
                    break;
                }
            }
            ////end compare centroids if didn't change then exit
        }
        ////end loop control

    }


    public static class my_mapper extends Mapper<Object,Text,Text,Text>
    {
        List<Text> my_cen=new ArrayList<Text>();

        ///get centroids
        protected void setup(Context context) throws IOException,InterruptedException
        {
            super.setup(context);
            Configuration conf=context.getConfiguration();
            Path cen_file_path=new Path(conf.get("centroids"));
            FileSystem my_fs=FileSystem.get(context.getConfiguration());
            BufferedReader my_br=new BufferedReader(new InputStreamReader(my_fs.open(cen_file_path)));

            try
            {
                String line;
                line=my_br.readLine();
                while(line!=null)
                {
                    my_cen.add(new Text(line));
                    line=my_br.readLine();
                }
            }
            finally
            {
                my_br.close();
            }
        }
        ///end get centroids

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException
        {
            String line=value.toString();
            int p_x=Integer.parseInt(line.split(",")[0]);
            int p_y=Integer.parseInt(line.split(",")[1]);

            String cur_cen=my_cen.get(0).toString();
            int cur_cen_x=Integer.parseInt(cur_cen.split(",")[0]);
            int cur_cen_y=Integer.parseInt(cur_cen.split(",")[1]);
            double cur_dis=Math.sqrt((Math.pow((cur_cen_x-p_x),2))+(Math.pow((cur_cen_y-p_y),2)));

            for(int i=1;i<my_cen.size();i++)
            {
                String i_cen=my_cen.get(i).toString();

                if(i_cen.length()<1)
                {
                    break;
                }

                int i_cen_x=Integer.parseInt(i_cen.split(",")[0]);
                int i_cen_y=Integer.parseInt(i_cen.split(",")[1]);

                double i_dis=Math.sqrt((Math.pow((i_cen_x-p_x),2))+(Math.pow((i_cen_y-p_y),2)));

                if(i_dis<cur_dis)
                {
                    cur_cen=i_cen;
                }
            }
            context.write(new Text(cur_cen),value);
        }
    }


    public static class my_combiner extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException
        {
            int count=0,sum_x=0,sum_y=0;

            int new_key=0;//in order for all value to go to the same reducer

            for(Text each_value:values)
            {
                String each_line=each_value.toString();
                int p_x=Integer.parseInt(each_line.split(",")[0]);
                int p_y=Integer.parseInt(each_line.split(",")[1]);

                //calculate count,sum_x,sum_y
                count+=1;
                sum_x+=p_x;
                sum_y+=p_y;
            }
            String new_val="";
            new_val+=key.toString();
            new_val+=",";
            new_val+=count;
            new_val+=",";
            new_val+=sum_x;
            new_val+=",";
            new_val+=sum_y;

            context.write(new Text(Integer.toString(new_key)), new Text(new_val));
        }
    }


    public static class my_reducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            Map<String,My_values> my_dict=new HashMap<String,My_values>();

            for(Text each_val:values)
            {
                String each_line=each_val.toString();
                String each_key_x=each_line.split(",")[0];
                String each_key_y=each_line.split(",")[1];
                String each_key=each_key_x+","+each_key_y;
                int each_count=Integer.parseInt(each_line.split(",")[2]);
                int each_sum_x=Integer.parseInt(each_line.split(",")[3]);
                int each_sum_y=Integer.parseInt(each_line.split(",")[4]);

                My_values my_val=new My_values();
                my_val.count_1=each_count;
                my_val.sum_x_1=each_sum_x;
                my_val.sum_y_1=each_sum_y;

                //check if key exits,if true merge values
                boolean if_exist=my_dict.containsKey(each_key);
                if(if_exist!=true)
                {
                    //create new
                    my_dict.put(each_key,my_val);
                }
                else
                {
                    My_values update_val=new My_values();

                    update_val=my_dict.get(each_key);
                    update_val.count_1+=each_count;
                    update_val.sum_x_1+=each_sum_x;
                    update_val.sum_y_1+=each_sum_y;

                    my_dict.replace(each_key,update_val);
                }
            }

            String ans="";
            //for each key calculate new centroid
            for(String key1:my_dict.keySet())
            {
                My_values final_val;//=new My_values();

                final_val=my_dict.get(key1);

                int final_x=final_val.sum_x_1/final_val.count_1;
                int final_y=final_val.sum_y_1/final_val.count_1;

                ans+=final_x;
                ans+=",";
                ans+=final_y;
                ans+="\n";
            }

            Text ans_txt=new Text();
            ans_txt.set(ans);

            context.write(null,ans_txt);
        }
    }

}