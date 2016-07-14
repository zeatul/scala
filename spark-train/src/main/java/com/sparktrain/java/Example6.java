package com.sparktrain.java;

import java.util.Arrays;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Charpter 6. Advanced Spark Programming
 * @author pzhang1
 *
 */
public class Example6 {

	private static SparkConf conf = new SparkConf().setAppName("MyApp")
			.setMaster("local");

	private static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	/**
	 * Example 6-4. Accumulator empty line count in Java
	 */
	public static void example6_4(){
		JavaRDD<String> rdd = sc.textFile("input.txt");
		final Accumulator<Integer> blankLines = sc.accumulator(0);
		JavaRDD<String> callSigns = rdd.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				if (line.equals(""))
					blankLines.add(1);
				return Arrays.asList(line.split(" "));
				
			}
		});
		/**
		 * we will see the right count only after we run the saveAsTextFile() action, because the transformation above it, map(), is lazy
		 */
		callSigns.saveAsTextFile("output.txt");
		System.out.println("Blank lines: "+ blankLines.value());
	}

}
