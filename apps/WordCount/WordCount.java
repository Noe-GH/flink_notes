package p1;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
// To use the basic dataset abstraction of the code
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
  public static void main(String[] args)
    throws Exception
  {
	// The environment is the context in which the program is excecuted.
	// It provides methods to control the job excecution.
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    
    // This class provides simple utility methods for reading and parsing the program
    // arguments. In this example, it is provided the input and output paths for the file
    // arguments.
    ParameterTool params = ParameterTool.fromArgs(args);
    
    // This is to make the parameters available and known in each and every node.
    env.getConfig().setGlobalJobParameters(params);
    
    DataSet<String> text = env.readTextFile(params.get("input"));
    
    DataSet<String> filtered = text.filter(new FilterFunction<String>()
    
    {
      public boolean filter(String value)
      {
        return value.startsWith("N");
      }
    });
    DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
    
    // groupBy(0) refers to group by the first tuple element
    // It is possible to perform operations using .
    // It is summing tuple field 1.
    DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(0).sum(1);
    
    //DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
    if (params.has("output"))
    {
      // The next writes the counts DataSet to a csv
      // Each row in the csv is configured to be terminated by new line, and fields terminated by space.
      counts.writeAsCsv(params.get("output"), "\n", " ");
      
      // Executing the program
      // Trigger for execution. If this method is not called, no operation will be performed.
      // If it is not called, the program will complete without doing anything 
      env.execute("WordCount Example");
    }
  }
  
  public static final class Tokenizer
    implements MapFunction<String, Tuple2<String, Integer>>
  {
    public Tuple2<String, Integer> map(String value)
    {
      return new Tuple2(value, Integer.valueOf(1));
    }
  }
}
