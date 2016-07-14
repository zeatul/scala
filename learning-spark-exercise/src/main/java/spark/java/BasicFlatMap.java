package spark.java;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class BasicFlatMap {

	public static void main(String[] args) throws Exception {
		String master;
		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local";
		}

		JavaSparkContext sc = new JavaSparkContext(master, "BasicMap",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));
		
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello wordl","hi"));
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}});
		words.first();
	}
	

}
