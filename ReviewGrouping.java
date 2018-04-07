

        import java.io.IOException;
        import java.util.*;
        
        import java.io.BufferedWriter;
        import java.io.File;
        
        import java.io.FileOutputStream;
        import java.io.IOException;
        import java.io.OutputStreamWriter;

        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.conf.*;
        import org.apache.hadoop.io.*;
        import org.apache.hadoop.mapreduce.*;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

        public class ReviewGrouping {

         public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            private Text one = new Text();
            private Text word = new Text();
            String delimiter=":";
            String final_value=new String()
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString()
            				if(line.indexOf(delimiter)>1)
            				{ 
                       
            					StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
                      String first_token,second_token=null;
                    					while (tokenizer.hasMoreTokens()) 
                    					{
                    						      first_token=tokenizer.nextToken();
                                       switch(first_token)
                          			      			    	{
                          			      			    		case "product/productId": 
                                                        if(tokenizer.hasMoreTokens()) 
                                                        {                                                            
                                                            second_token=tokenizer.nextToken();                                          
                                                            word.set(second_token);
                                                         }    
                          			      			    			  break;
                          			      			    		case "review/userId":
                                                      if(tokenizer.hasMoreTokens()) 
                                                      {                                                                                          
                                                       second_token=tokenizer.nextToken();
                                                       final_value=second_token+" ";
                                                      } 
                          			      			    			break;
                          			      			    		case "review/score":
                                                      if(tokenizer.hasMoreTokens()) 
                                                      {                                                                                           
                                                         second_token=tokenizer.nextToken();
                            			      			    			 final_value=final_value+second_token;
                            										         context.write(word,new Text(final_value));
                                                         final_value=new String();  
                                                       }                                                                                           
                                			    		         break;
                          			      			    		default:
                          			      			    			;
                  			    	         			     }
     					 				         }
                        
                        }	
                
                }
        
         } 

         public static class Reduce extends Reducer<Text, Text, Text, Text> {

            public void reduce(Text key, Iterable<Text> values, Context context) 
              throws IOException, InterruptedException {
                
                for (Text val : values) 
                context.write(key, new Text(val));
            } 
         }
            
         public static class MapCanopies extends Mapper<LongWritable, Text, Text, Text> {
           
            Hashtable<String, String> userRatings=new Hashtable<String, String>();   
            Hashtable<String, Hashtable> productUser=new Hashtable<String, Hashtable>();
            ArrayList<String>  canopy_kcenters =new ArrayList<String>();
            String productId=new String();
            
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
            {
              String line = value.toString(),token,userId=null,rating=null;
              StringTokenizer tokenizer = new StringTokenizer(line);
              if(tokenizer.countTokens()>=3)
              {
               int token_no=1;
               while(tokenizer.hasMoreTokens()) {
                 
                 if(token_no==1)
                  {
                    token=tokenizer.nextToken();
                      
                      if(token.equals(productId))
                            ;
                        else
                          {
                            if(canopy_kcenters.isEmpty()) 
                              {
                               productId=token;
                               canopy_kcenters.add(productId);
                               
                              } 
                              else if(canopy_kcenters.size()==1 && productUser.isEmpty())
                               {
                                 
                                 productUser.put(productId,userRatings);
                                 
                                 Set<String> userIds=userRatings.keySet();
                                 for(String user_id:userIds)
                                 context.write(new Text(productId), new  Text(user_id+" "+userRatings.get(user_id)));
                                 
                                 userRatings=new Hashtable();
                               }
                              
                              else
                               {
                                  Iterator<String> iterator = canopy_kcenters.iterator();
                                  String k_center=new String();
                                   int canopy_metric=0;
		    		                       boolean got_atleast_one_canopy=false;
                                   Hashtable<String,String> usersinKcenter=null;                                                           
                                  while (iterator.hasNext()) 
                                  {
                                		k_center=iterator.next();
                                    usersinKcenter= productUser.get(k_center);
                                    Set<String> users=usersinKcenter.keySet();
                                    
                                    for(String key1: users)
                						    		{
                						    			if(userRatings.containsKey(key1.trim()))
                						    				canopy_metric++;
                						    		}
                                    
                                    if(canopy_metric>2)
                                     got_atleast_one_canopy=true;
                                    
                                   }
                                 
                                  if(!got_atleast_one_canopy) 
                                     {
                                       canopy_kcenters.add(productId);
                                       productUser.put(productId,userRatings);
                                       
                                       Set<String> userIds=userRatings.keySet();
                                        for(String user_id:userIds)
                                        context.write(new Text(productId), new  Text(user_id+" "+userRatings.get(user_id)));
                                       
                                     } 
                                  productId=token; 
                                  userRatings=new Hashtable();        
                               }
                          
                          }
                       
                        
                        token_no++;
                  }
                  
                  if(token_no==2)
                  {
                     userId=tokenizer.nextToken().trim();
                     token_no++;
                  }  
                    
                  if(token_no==3)  
                   {
                      rating=tokenizer.nextToken().trim();
                      userRatings.put(userId,rating);
                   }  
                    
                }
              
              }
            
            }
            
         }
         
         //----------------------------------//
          public static class ReduceCanopies extends Reducer<Text, Text, Text, Text> 
		      {

            Hashtable<String, String> userRatings=new Hashtable<String, String>();   
            Hashtable<String, Hashtable> productUser=new Hashtable<String, Hashtable>();
            ArrayList<String>  canopy_kcenters =new ArrayList<String>();
            String productId=new String();
            Integer canopy_cnt=new Integer(1);
            Text final_value=new Text();
            public void reduce(Text key, Iterable<Text> values, Context context) 
              throws IOException, InterruptedException 
			      {
                 productId=key.toString();
                 String user_id=new String(),rate=new String();
                  
                  if(canopy_kcenters.size()==0)
    						   {
          							 canopy_kcenters.add(productId);
          							 
          							 productUser.put(productId,new Hashtable());
          									 for(Text val : values)
          									 {
          									   StringTokenizer str=new StringTokenizer(val.toString());
                               if(str.hasMoreTokens()) 
                                user_id=str.nextToken();
                               if(str.hasMoreTokens())  
          									   rate=str.nextToken();
          									   productUser.get(productId).put(user_id,rate);
          									   
          									 } 
                              final_value.set(canopy_cnt.toString());                                                                  
                              context.write(key,final_value);
                              canopy_cnt++;                                                               
    						   
                   }
                   else
                   {
                                   Iterator<String> iterator = canopy_kcenters.iterator();
                                   String k_center=new String();
                                   int canopy_metric=0;
		    		                       boolean got_atleast_one_canopy=false;
                                   Hashtable<String,String> usersinKcenter=null;                                                           
                                  
                									  while (iterator.hasNext()) 
                									  {
                											k_center=iterator.next();
                											usersinKcenter= productUser.get(k_center);
                											Set<String> users=usersinKcenter.keySet();
                										
     													        for(Text val : values)
                        									 {
                        									   StringTokenizer str=new StringTokenizer(val.toString());
                                             if(str.hasMoreTokens())                                                                                         
                                             user_id=str.nextToken();
                                             if(str.hasMoreTokens())
                        									   rate=str.nextToken();
                                              if(users.contains(val.toString()))                                                                                                                                  canopy_metric++;                                                       
                        									   
                        									 } 
                                                                                                                                                                                                      
                										
                											if(canopy_metric>2)
                											 got_atleast_one_canopy=true;
                										
                									   }
                                 
                                  if(!got_atleast_one_canopy) 
                                     {
                                        canopy_kcenters.add(productId);
							                          
							                           productUser.put(productId,new Hashtable());
                  										 for(Text val : values)
                  										 {
                  										   StringTokenizer str=new StringTokenizer(val.toString());
                                                                                                                                                                                                          if(str.hasMoreTokens())                           
                  										   user_id=str.nextToken();
                                         if(str.hasMoreTokens())                           
                  										   rate=str.nextToken();
                  										   productUser.get(productId).put(user_id,rate);
                  										   //context.write(key, val);
                  										 } 
                                      final_value.set(canopy_cnt.toString());                                                                  
                                      context.write(key,final_value );
                                      canopy_cnt++;
                                     
                                   } 
                                           
                             }
                          
                          
                        }
                    
		         }
                
         
         
         
         
         
         
         
         
         //---------------------------------//
         
         public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Configuration conf1 = new Configuration();
            Job job = new Job(conf, "vamsi1");

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setJarByClass(VamsiTest.class);
            

            job.waitForCompletion(true);
            
            Job job1 = new Job(conf1, "vamsi2");

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setNumReduceTasks(1);
            job1.setMapperClass(MapCanopies.class);
            job1.setReducerClass(ReduceCanopies.class);

            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));

            job1.setJarByClass(VamsiTest.class);
            

            job1.waitForCompletion(true);
         }

        }
        