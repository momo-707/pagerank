import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pa {

    /*map过程*/
    public static class Pamapper extends Mapper<Object,Text,Text,Text>{        
        private String id;
        private float pr;       
        private int count;
        private float average_pr;       
        public void map(Object key,Text value,Context context)
            throws IOException,InterruptedException{            
            StringTokenizer str = new StringTokenizer(value.toString());
            id =str.nextToken();
            pr = Float.parseFloat(str.nextToken());
            count = str.countTokens();//当前页面的出链数
            average_pr = pr/count;//当前页面对出链网页的贡献值
            //用符号"%"和"#"来区分贡献值和出链网页
            String linkpages ="%";
            while(str.hasMoreTokens()){
                String linkpage = str.nextToken();
                context.write(new Text(linkpage),new Text("#"+average_pr));//输出键值对为<出链网页，获得的贡献值>
                linkpages +=" "+ linkpage;
            }       
            context.write(new Text(id), new Text(linkpages));//输出键值对为<当前网页，所有出链网页>
        }       
    }

    /*reduce过程*/
    public static class Pareduce extends Reducer<Text,Text,Text,Text>{     
        public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{            
            String link = "";
            float pr = 0;
            //对values中的元素进行归类，得到sum贡献值和所有出链网页
            for(Text v:values){
                if(v.toString().substring(0,1).equals("%")){
                    link += v.toString().substring(1);
                }

                else if(v.toString().substring(0,1).equals("#")){
                    pr += Float.parseFloat(v.toString().substring(1));
                }
            }

            pr = 0.85f*pr + 0.15f*0.25f; //加上阻尼系数重算贡献值         
            String result = pr+link;
            context.write(key, new Text(result));           
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String pathIn = "/home/hadoop/VirtualLab/pr/input/pagerank.txt";
        String pathOut = "/home/hadoop/VirtualLab/pr/output";
        for(int i=1;i<41;i++){      
            Job job = Job.getInstance(conf,"PageRank");
            job.setJarByClass(Pa.class);
            job.setMapperClass(Pamapper.class);
            job.setReducerClass(Pareduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(pathIn));
            FileOutputFormat.setOutputPath(job, new Path(pathOut));
            pathIn = pathOut;//将本次输出作为下一次的输入
            pathOut = pathOut+i;//设置下次的输出为新的地址
            job.waitForCompletion(true);
        }
    }
}



