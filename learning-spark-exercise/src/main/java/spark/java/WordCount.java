package spark.java;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		SparkConf conf = new SparkConf().setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		// split u into words.
		JavaRDD<String> words = input
				.flatMap(new FlatMapFunction<String, String>() {

					@Override
					public Iterable<String> call(String x) throws Exception {
						return Arrays.asList(x.split(" "));
					}
				}

				);
		// Transform into parirs and count
		JavaPairRDD<String, Integer> counts = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String x)
							throws Exception {
						return new Tuple2(x, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}

}
