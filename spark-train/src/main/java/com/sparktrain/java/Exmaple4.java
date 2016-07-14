package com.sparktrain.java;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;









public class Exmaple4 {
	
	private static SparkConf  conf = new SparkConf().setAppName("MyApp").setMaster("local");
	
	private static JavaSparkContext sc = new JavaSparkContext(conf);
	
	public static void main(String[] args){
		
	}
	
	/**
	 * Example 4-3. Creating a pair RDD using the first word as the key in Java
	 */
	public static JavaPairRDD<String,String> example4_3(){
		JavaRDD<String>  lines = sc.parallelize(Arrays.asList("hello world","TMD Go to hell"));
		
		PairFunction<String, String, String> keyData  = null;
		
		keyData = new PairFunction<String, String, String>(){

			@Override
			public Tuple2<String, String> call(String x) throws Exception {
				return new Tuple2(x.split(" ")[0], x);
			}
			
		};
		
		JavaPairRDD<String,String> pairs = lines.mapToPair(keyData);
		
		return pairs;
	}

	/**
	 * Example 4-6. Simple filter on second element in Java
	 */
	public static void example4_6(){
		Function<Tuple2<String,String>, Boolean> longWordFilter = new Function<Tuple2<String,String>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, String> keyValue) throws Exception {
				return keyValue._2().length() <20;
			}
		};
		
		JavaPairRDD<String,String> pairs = example4_3();
		
		JavaPairRDD<String,String> result = pairs.filter(longWordFilter);
	}
	
	/**
	 * Example 4-11. Word count in Java
	 */
	public static void example4_11(){
		JavaRDD<String> input = sc.textFile("s3://........");
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String,Integer> result = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String x) throws Exception {
				return new Tuple2<String, Integer>(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a+b;
			}
		});
	}
	
	/**
	 * Example 4-14. Per-key average using combineByKey() in Java
	 */
	public static void example4_14(){
		JavaPairRDD<String,Integer> input = sc.parallelizePairs(Arrays.asList(new Tuple2<String,Integer>("panda", 0),
				new Tuple2<String,Integer>("pink", 3),
				new Tuple2<String,Integer>("pirate", 3),
				new Tuple2<String,Integer>("panda", 1),
				new Tuple2<String,Integer>("pink", 4)));
		
		
		class AvgCount implements Serializable{
			
			public int total_;
			public int num_;
			public AvgCount(int total ,int num){
				this.total_ = total;
				this.num_ = num;
			}
			public float avg() { return total_ / (float) num_; }
		}
	
		Function<Integer, AvgCount> createAcc = new Function<Integer,AvgCount>() {

			@Override
			public AvgCount call(Integer x) throws Exception {
				return new AvgCount(x, 1);
			}
		};
		
		Function2<AvgCount,Integer,AvgCount> addAndcount = new Function2<AvgCount,Integer,AvgCount>() {
			public AvgCount call(AvgCount a, Integer x) throws Exception {
				a.num_ += 1;
				a.total_ += x;
				return a;
			}
		};
		
		Function2<AvgCount,AvgCount,AvgCount> combine = new Function2<AvgCount,AvgCount,AvgCount>() {

			@Override
			public AvgCount call(AvgCount a, AvgCount b) throws Exception {
				a.total_ += b.total_;
				a.num_ += b.num_;
				return a;
			}
		};		
	
		
		JavaPairRDD<String,AvgCount> avgCounts = input.combineByKey(createAcc, addAndcount, combine);
	
		Map<String,AvgCount> countMap = avgCounts.collectAsMap();
		
		for (Entry<String,AvgCount> entry : countMap.entrySet()){
			System.out.println(entry.getKey()+ ":" + entry.getValue().avg());
		}
		
	}

	/**
	 * Example 4-21. Custom sort order in Java, sorting integers as if strings
	 */
	public static void example4_21(){
		class IntegerComparator implements Comparator<Integer>{

			@Override
			public int compare(Integer a, Integer b) {
				return String.valueOf(a).compareTo(String.valueOf(b));
			}
			
		}
		
		JavaPairRDD<Integer,String> input = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer,String>(100,"100"),
				new Tuple2<Integer,String>(99,"99")));
		
		input.sortByKey(new IntegerComparator());
	}

	
}
