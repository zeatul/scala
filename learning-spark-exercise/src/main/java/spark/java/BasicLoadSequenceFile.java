package spark.java;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;



/**
 * Example 5-22. Loading a SequenceFile in Java
 * 
 * @author pzhang1
 *
 */
public class BasicLoadSequenceFile {
	
	public static class ConvertToNativetypes implements PairFunction<Tuple2<Text,IntWritable>,String,Integer>{

		@Override
		public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record)
				throws Exception {
			return new Tuple2(record._1.toString(),record._2.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception(
					"Usage BasicLoadSequenceFile [sparkMaster] [input]");
		}
		String master = args[0];
		String fileName = args[1];

		JavaSparkContext sc = new JavaSparkContext(master,
				"basicloadsequencefile", System.getenv("SPARK_HOME"),
				System.getenv("JARS"));
		
		JavaPairRDD<Text,IntWritable> input = sc.sequenceFile(fileName, Text.class, IntWritable.class);
		JavaPairRDD<String,Integer> result = input.mapToPair(new ConvertToNativetypes());
	}

}
