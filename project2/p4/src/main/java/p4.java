
////reference: http://people.csail.mit.edu/lcao/papers/DOD.pdf

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

class Points
{
    public int x_1;
    public int y_1;
}
class Rows
{
    public Points[] row;
}
class Cells implements java.io.Serializable
{
    public Points b_l;
    public Points b_r;
    public Points t_l;
    public Points t_r;
}


public class p4
{

    public static void main(String[] args) throws Exception
    {
        Configuration conf=new Configuration();

        ////two mandatory parameters r and k
        //conf.set("r_radius",args[1]);
        //conf.set("k_neighbor",args[2]);
        conf.set("r_radius",Integer.toString(1));
        conf.set("k_neighbor",Integer.toString(2));
        ////end two mandatory parameters r and k


        ////dividing cells
        int bound=10000;
        int my_size=100;
        /////for test purpose comment out the last two lines and use the next two lines
        //int bound=5;
        //int my_size=1;

        int cell_bound=bound/my_size;

        Rows[] my_rows=new Rows[cell_bound];

        //all the points row by row saved in my_rows
        for(int i=0;i<cell_bound;i++)
        {
            Points[] my_points=new Points[cell_bound];

            //each point in a single row
            for(int j=0;j<cell_bound;j++)
            {

                Points my_point=new Points();
                my_point.x_1=1+my_size*j;
                my_point.y_1=bound-my_size*i;

                my_points[j]=new Points();
                my_points[j]=my_point;
            }

            my_rows[i]=new Rows();
            my_rows[i].row=my_points;
        }

        Cells[] my_cells=new Cells[cell_bound*cell_bound];
        int count=0;
        String all_cells="";
        for(int i=0;i<cell_bound-1;i++)
        {
            //i for current row, i+1 next row, whcih are the two rows needed to form cell
            for(int j=0;j<cell_bound-1;j++)
            {

                my_cells[count]=new Cells();
                //for each points in that particular row
                my_cells[count].t_l=my_rows[i].row[j];//e.g.first row, first point
                my_cells[count].t_r=my_rows[i].row[j+1];//e.g.first row, second point
                my_cells[count].b_l=my_rows[i+1].row[j];//e.g.second row, first point
                my_cells[count].b_r=my_rows[i+1].row[j+1];//e.g.second row, second point
                count++;
            }
        }

        String all_cells_to_str="";

        for(int i=0;i<(cell_bound-1)*(cell_bound-1);i++)
        {
            String temp="";

            temp+=my_cells[i].t_l.x_1;
            temp+=",";
            temp+=my_cells[i].t_l.y_1;
            temp+=" ";

            temp+=my_cells[i].t_r.x_1;
            temp+=",";
            temp+=my_cells[i].t_r.y_1;
            temp+=" ";

            temp+=my_cells[i].b_l.x_1;
            temp+=",";
            temp+=my_cells[i].b_l.y_1;
            temp+=" ";

            temp+=my_cells[i].b_r.x_1;
            temp+=",";
            temp+=my_cells[i].b_r.y_1;
            temp+=" ";

            temp+=";";

            all_cells_to_str+=temp;
        }
        /*
        String serializedCells="";
        for(int i=0;i<cell_bound*cell_bound;i++)
        {
            try
            {
                ByteArrayOutputStream b_o=new ByteArrayOutputStream();
                ObjectOutputStream s_o=new ObjectOutputStream(b_o);
                s_o.writeObject(my_cells[i]);
                s_o.flush();
                serializedCells+=b_o.toString();
            }
            catch (Exception e)
            {
                System.out.println(e);
            }
        }

        conf.set("my_cells",serializedCells);
		*/

        conf.set("my_cells",all_cells_to_str);

        conf.set("cell_bound",Integer.toString(cell_bound));
        ////end dividing cells

        Job job=Job.getInstance(conf,"p4");
        job.setJarByClass(p4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(my_mapper.class);

        job.setReducerClass(my_reducer.class);

        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, my_mapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //FileOutputFormat.setOutputPath(job, new Path(args[3]));

        ///achieve scalability by setting number of map and reduce
        //job.setNumMapTasks(20);
        //job.setMaxInputSplitSize(500);
        job.setNumReduceTasks(10);
        ///

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class my_mapper extends Mapper<Object,Text,Text,Text>
    {
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException
        {

            String line=value.toString();
            int p_x=Integer.valueOf(line.split(",")[0].trim());
            int p_y=Integer.valueOf(line.split(",")[1].trim());

            //get my_cells from main
            Configuration conf=context.getConfiguration();

            int get_r=Integer.valueOf(conf.get("r_radius").trim());
            int get_cell_bound=Integer.valueOf(conf.get("cell_bound").trim());

            String get_all_cells_to_str=conf.get("my_cells").trim();
            Cells[] get_my_cells=new Cells[get_cell_bound*get_cell_bound];
            Points[] get_points=new Points[get_cell_bound];

            for(int i=0;i<(get_cell_bound-1)*(get_cell_bound-1);i++)
            {
                String single_cell_str="";
                single_cell_str=get_all_cells_to_str.split(";")[i];

                get_my_cells[i]=new Cells();

                int count_get_p=0;

                for(int j=0;j<4;j++)
                {
                    get_points[count_get_p]=new Points();

                    String temp=single_cell_str.split(" ")[j];
                    String temp_x=temp.split(",")[0];
                    String temp_y=temp.split(",")[1];

                    get_points[count_get_p].x_1=Integer.parseInt(temp_x);
                    get_points[count_get_p].y_1=Integer.parseInt(temp_y);

                    count_get_p++;
                }

                get_my_cells[i].t_l=get_points[0];
                get_my_cells[i].t_r=get_points[1];
                get_my_cells[i].b_l=get_points[2];
                get_my_cells[i].b_r=get_points[3];
            }

            /*
            String get_Cells=conf.get("my_cells");

            String get_serializedCells=conf.get("my_cells");

            Cells[] get_my_cells=new Cells[get_cell_bound*get_cell_bound];

            for(int i=0;i<get_cell_bound*get_cell_bound;i++)
            {
                try
                {
                    byte b[]=get_serializedCells.getBytes();
                    ByteArrayInputStream b_i=new ByteArrayInputStream(b);
                    ObjectInputStream s_i=new ObjectInputStream(b_i);
                    //Cells[] get_my_cells=new Cells[get_cell_bound*get_cell_bound];
                    Cells new_cell=(Cells) s_i.readObject();
                    get_my_cells[i]=new_cell;
                }
                catch(Exception e)
                {
                    System.out.println(e);
                }
            }
            */

            //String key_out="";
            List<Integer> key_out=new ArrayList<Integer>();
            int count_k_o=0;


            for(int i=0;i<(get_cell_bound-1)*(get_cell_bound-1);i++)
            {
                String val_out="";
                //check in which cell p is a core point
                //basically check for p does it match the requirenment for a paticular cell
                if(
                        (p_x>=get_my_cells[i].b_l.x_1)&&(p_x<=get_my_cells[i].b_r.x_1)
                                &&(p_y>=get_my_cells[i].b_l.y_1)&&(p_y<=get_my_cells[i].t_l.y_1)
                )
                {
                    //yes p is in this cell
                    //so we need to create key value pair with tag "0" since this is core point
                    //key_out+=Integer.toString(i);
                    key_out.add(i);
                    count_k_o++;

                    val_out+="0";
                    val_out+=",";
                    val_out+=Integer.toString(p_x);
                    val_out+=",";
                    val_out+=Integer.toString(p_y);
                    //val_out+=";";


                }
                //since p is not a core point
                //check in which cell p is a support point
                //note we need to check in new_area which also contains the original area
                //but since p is not in original area
                //we don't need to check again or new area-the original area
                //we can just use the whole new area
                else
                {
                    if(
                            (p_x>=(get_my_cells[i].b_l.x_1-get_r))&&(p_x<=(get_my_cells[i].b_r.x_1+get_r))
                                    &&(p_y>=(get_my_cells[i].b_l.y_1-get_r))&&(p_y<=(get_my_cells[i].t_l.y_1+get_r))
                    )
                    {
                        //p is a support point for this particular cell
                        //key_out+=Integer.toString(i);
                        key_out.add(i);
                        count_k_o++;

                        val_out+="1";
                        val_out+=",";
                        val_out+=Integer.toString(p_x);
                        val_out+=",";
                        val_out+=Integer.toString(p_y);
                        //val_out+="|";
                    }
                }
                context.write(new Text(Integer.toString(i)),new Text(val_out));
            }
        }
    }


    public static class my_reducer extends Reducer<Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            Configuration conf=context.getConfiguration();
            int get_r=Integer.valueOf(conf.get("r_radius").trim());
            int get_k=Integer.valueOf(conf.get("k_neighbor").trim());

            //List<String> points_arr=new ArrayList<String>();
            //int count_points_arr=0;

            String test="Cell ";
            test+=key;
            test+=" :";
            String cleaned_records="";
            //cleaned_records+=test;

            for(Text txtvalue:values)
            {
                String line=txtvalue.toString();

                String[] line_split=line.split(",");
                String one_record="";
                for(int i=0;i<line_split.length;i++)
                {
                    one_record+=line_split[i];
                    if(one_record.length()!=0)
                    {
                        one_record+=",";
                    }
                }
                //one_record+=one_record.length();
                if(one_record.length()!=0)
                {
                    cleaned_records+=one_record;
                    cleaned_records+=";";
                }
            }
            //context.write(null,new Text(cleaned_records));

            //String out_arr="";
            String final_ans="";
            //out_arr+=test;

            String[] cleaned_records_split=cleaned_records.split(";");
            String single_crs=""; String single_crs_1="";
            for(int i=0;i<cleaned_records_split.length;i++)
            {
                int count_neighbors=0;
                single_crs="";

                single_crs+=cleaned_records_split[i];

                //context.write(null,new Text(single_crs));

                if(single_crs.length()!=0)
                {
                    String[] scrs_clean=single_crs.split(",");
                    String scrsc_tag=scrs_clean[0];
                    String x_str=scrs_clean[1];
                    String y_str=scrs_clean[2];

                    int scrsc_x=Integer.parseInt(x_str);
                    int scrsc_y=Integer.parseInt(y_str);

                    for(int j=0;j<cleaned_records_split.length;j++)
                    {
                        single_crs_1="";
                        single_crs_1+=cleaned_records_split[j];

                        String[] scrs_clean_1=single_crs_1.split(",");
                        String scrsc_tag_1=scrs_clean_1[0];
                        String x_str_1=scrs_clean_1[1];
                        String y_str_1=scrs_clean_1[2];

                        int scrsc_x_1=Integer.parseInt(x_str_1);
                        int scrsc_y_1=Integer.parseInt(y_str_1);

                        if(i!=j)
                        {
                            if(Math.sqrt((Math.pow((scrsc_x_1-scrsc_x),2))+(Math.pow((scrsc_y_1-scrsc_y),2)))<=get_r)
                            {
                                count_neighbors++;
                            }
                        }
                    }
                    if(count_neighbors<get_k)
                    {
                        if(scrsc_tag.equals("0"))
                        {
                            final_ans+=test;
                            final_ans+=x_str;
                            final_ans+=",";
                            final_ans+=y_str;
                            final_ans+="\n";
                        }

                    }
                }
            }

            //context.write(null,new Text(final_ans));

        }
    }

}