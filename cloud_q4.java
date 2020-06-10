import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 public class ReduceJoin {
 public static class CustsMapper extends Mapper <Object, Text, Text, Text>
 {
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException 
 {
 String space=" ";
 String record = value.toString();
 String[] parts = record.split(",");
 /*String str="cust ";
 str=str.concat(parts[1]);
 str=str.concat(space);
  str=str.concat(parts[2]);
  str=str.concat(space);
 str=str.concat(parts[3]);
 str=str.concat(space);
 str=str.concat(parts[4]);*/
 context.write(new Text(parts[0]), new Text("cust " + parts[1]+ " " + parts[2]+" "+ parts[3]+" "+ parts[4]));
 //context.write(new Text(parts[0]), new Text(str)); 
 }
 }
 
 public static class TxnsMapper extends Mapper <Object, Text, Text, Text>
 {
 public void map(Object key, Text value, Context context) 
 throws IOException, InterruptedException 
 {
 String space=" ";
 String record = value.toString();
 String[] parts = record.split(",");
 /*String str="loanid ";
  str=str.concat(parts[0]);
 str=str.concat(space);
  str=str.concat(parts[1]);
  str=str.concat(space);
 str=str.concat(parts[2]);*/
 context.write(new Text(parts[3]), new Text("loanid " + parts[0]+" "+parts[1]+" "+parts[2]));
 // context.write(new Text(parts[3]), new Text(str));
 }
 }
 
 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
 {
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException 
 {
 String name = "";
 String nm="";
 String str= "";
 String space=" ";
 String newline="\n";
 String empty=" ";
 //double total = 0.0;
 int count = 0;
   int flg=0;

 


 for (Text t : values) 
 { 
 String parts[] = t.toString().split(" ");
 if (parts[0].equals("loanid")) 
 {
 //count++;
 //total += Float.parseFloat(parts[1]);
  // str=str.concat(name);
  // str=str.concat(space);
  // str=str.concat(parts[1]);
  // str=str.concat(space);
  // str=str.concat(parts[2]);
  // str=str.concat(space);
  // str=str.concat(parts[3]);
  // str=str.concat(newline);

   str = str + parts[1]+"\t"+ parts[2]+"\t"+ parts[3] + ",";
   count=count+1;
   //context.write(new Text(name),new Text(str));
  
  flg=1;
  //break;
 } 
 else if (parts[0].equals("cust")) 
 {
  /*name=name.concat(parts[1]);
  name=name.concat(space);
  name=name.concat(parts[2]);
  name=name.concat(space);
  name=name.concat(parts[3]);
  name=name.concat(space);
  name=name.concat(parts[4]);*/
 
 name = parts[1]+"\t"+ parts[2]+"\t"+ parts[3]+"\t"+ parts[4];
 nm=parts[1];
 }
 }
 if(flg==1 && nm.equals("reema"))
      { String part[] = str.split(",");
        
        for(int i=0;i<count;i++)
        { String p[]=part[i].split("\t");
          context.write(new Text(nm),new Text(p[1]));
        }


      }
 //String str = String.format("%d %f", count, total);
 //context.write(new Text(name), new Text(str));
 }
 }
 
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "Reduce-side join");
 job.setJarByClass(ReduceJoin.class);
 job.setReducerClass(ReduceJoinReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
  
 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
 Path outputPath = new Path(args[2]);
  
 FileOutputFormat.setOutputPath(job, outputPath);
 outputPath.getFileSystem(conf).delete(outputPath);
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 }