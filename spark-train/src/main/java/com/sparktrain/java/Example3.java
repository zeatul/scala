package com.sparktrain.java;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;

public class Example3 {

	private static SparkConf conf = new SparkConf().setMaster("local")
			.setAppName("MyApp");
	private static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {

		JavaRDD<String> inputRDD = sc.textFile("log.txt");
		JavaRDD<String> errorRDD = inputRDD
				.filter(new Function<String, Boolean>() {

					public Boolean call(String x) throws Exception {
						return x.contains("error");
					}
				});

		JavaRDD<String> warnRDD = inputRDD
				.filter(new Function<String, Boolean>() {

					@Override
					public Boolean call(String x) throws Exception {
						return x.contains("warn");
					}
				});

		JavaRDD<String> badLineRDD = errorRDD.union(warnRDD);

		System.out.println("Ipunt had " + badLineRDD.count()
				+ " concerning lines");
		for (String line : badLineRDD.take(10)) {
			System.out.println(line);
		}

	}

	/**
	 * Example 3-28. Java squaring the values in an RDD
	 */
	public static void example3_28() {
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = input.map(new Function<Integer, Integer>() {

			@Override
			public Integer call(Integer x) throws Exception {
				return x * x;
			}
		});

		System.out.println(StringUtils.join(result.collect(), ","));
	}

	/**
	 * *Example 3-31. flatMap() in Java, splitting lines into multiple words
	 */
	public static void example3_31() {
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world",
				"hi"));
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {

					@Override
					public Iterable<String> call(String line) {
						return Arrays.asList(line.split(" "));
					}

				});
		words.first();
	}

	/**
	 * Example 3-37. aggregate() in Java
	 */
	public static void example3_37() {
		class AvgCount implements Serializable {
			public AvgCount(int total, int num) {
				this.total = total;
				this.num = num;
			}

			public int total;
			public int num;

			public double avg() {
				return total / (double) num;
			}
		}
		
		Function2<AvgCount,Integer,AvgCount> addAndCount =
				new Function2<AvgCount,Integer,AvgCount>(){

					@Override
					public AvgCount call(AvgCount a, Integer x)
							throws Exception {
						a.total += x;
						a.num+=1;
						return a;
					}
			
		};
		
		Function2<AvgCount, AvgCount, AvgCount> combine =
			new	Function2<AvgCount, AvgCount, AvgCount>(){

				@Override
				public AvgCount call(AvgCount a, AvgCount b)
						throws Exception {
					a.total += b.total;
					a.num += b.num;
					return a;
				}
		
		};
		
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		AvgCount initial = new AvgCount(0, 0);
		AvgCount result = input.aggregate(initial, addAndCount, combine);
		System.out.println(result.avg());
	}

	/**
	 * Example 3-38. Creating DoubleRDD in Java
	 */
	public static void example3_38(){
		JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaDoubleRDD result =  input.mapToDouble(new DoubleFunction<Integer>() {			
			@Override
			public double call(Integer x) throws Exception {
				return x*x;
			}
		});
	}

}
