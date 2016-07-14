package com.sparktrain.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import au.com.bytecode.opencsv.CSVReader;

import java.io.StringReader;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

/**
 * Charpeter 5 loading and saving your data
 */
public class Example5 {

	private static SparkConf conf = new SparkConf().setAppName("MyApp")
			.setMaster("local");

	private static JavaSparkContext sc = new JavaSparkContext(conf);

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * Example 5-3. Loading a text file in Java
	 */
	public static void example5_3() {
		JavaRDD<String> input = sc
				.textFile("file:///home/holden/repos/spark/README.md");
	}

	public static class Person {

	}

	/**
	 * Example 5-8. Loading JSON in Java 
	 * Example 5-11. Saving JSON in Java
	 */
	public static void example5_8() {

		class ParseJson implements FlatMapFunction<Iterator<String>, Person> {

			@Override
			public Iterable<Person> call(Iterator<String> lines)
					throws Exception {
				ArrayList<Person> people = new ArrayList<Person>();
				ObjectMapper mapper = new ObjectMapper();
				while (lines.hasNext()) {
					String line = lines.next();
					try {
						people.add(mapper.readValue(line, Person.class));
					} catch (Exception e) {

					}
				}

				return people;
			}

		}

		// load
		JavaRDD<String> input = sc.textFile("file.json");
		JavaRDD<Person> result = input.mapPartitions(new ParseJson());

		class WriteJson implements FlatMapFunction<Iterator<Person>, String> {

			@Override
			public Iterable<String> call(Iterator<Person> people)
					throws Exception {
				ArrayList<String> text = new ArrayList<String>();
				ObjectMapper mapper = new ObjectMapper();
				while (people.hasNext()) {
					Person person = people.next();
					text.add(mapper.writeValueAsString(person));
				}
				return text;
			}

		}

		// write
		JavaRDD<Person> filteredPerson = result
				.filter(new Function<Example5.Person, Boolean>() {
					@Override
					public Boolean call(Person arg0) throws Exception {
						return true;
					}
				});
		
		JavaRDD<String> formatted = filteredPerson.mapPartitions(new WriteJson());
		formatted.saveAsTextFile("outfile");
	}

	/**
	 * Example 5-14. Loading CSV with textFile() in Java
	 */
	public static void example5_14(){
		class ParseLine implements Function<String, String[]>{

			@Override
			public String[] call(String line) throws Exception {
				CSVReader reader = new CSVReader(new StringReader(line));
				return reader.readNext();
			}			
		}
		
		JavaRDD<String> csvFile1 = sc.textFile("inputFile");
		JavaRDD<String[]> csvData = csvFile1.map(new ParseLine());
	}


	/**
	 * Example 5-17. Loading CSV in full in Java
	 * If there are embedded newlines in fields, we will need to load each file in full and parse the entire segment
	 */
	public static void example5_17(){
		class ParseLine implements FlatMapFunction<Tuple2<String,String>, String[]>{

			@Override
			public Iterable<String[]> call(Tuple2<String, String> file)
					throws Exception {
				CSVReader reader = new CSVReader(new StringReader(file._2()));
				return reader.readAll();
			}
			
		}
		JavaPairRDD<String,String> csvData = sc.wholeTextFiles("inputFile");
		JavaRDD<String[]> keyRDD = csvData.flatMap(new ParseLine());
	}
	
	/**
	 * Example 5-22. Loading a SequenceFile in Java
	 */
	public static void example5_22(){
		class ConvertToNativeTypes implements PairFunction<Tuple2<Text,IntWritable>, String, Integer>{

			@Override
			public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record)
					throws Exception {
				return new Tuple2(record._1.toString(), record._2.get());
			}
			
		}
		
		JavaPairRDD<Text,IntWritable> input = sc.sequenceFile("fileName", Text.class, IntWritable.class);
		
		JavaPairRDD<String,Integer> result = input.mapToPair(new ConvertToNativeTypes());
	}
	
	/**
	 * Example 5-26. Saving a SequenceFile in Java
	 */
	public static void exmaple5_26(){
		class ConvertToWritableTypes implements  PairFunction<Tuple2<String,Integer>, Text, IntWritable>{

			@Override
			public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record)
					throws Exception {
				return new Tuple2(new Text(record._1), new IntWritable(record._2));
			}
			
		}
		
		@SuppressWarnings("unchecked")
		JavaPairRDD<String,Integer> input = sc.parallelizePairs(Arrays.asList(new Tuple2<String,Integer>("panda", 0),
				new Tuple2<String,Integer>("pink", 3),
				new Tuple2<String,Integer>("pirate", 3),
				new Tuple2<String,Integer>("panda", 1),
				new Tuple2<String,Integer>("pink", 4)));
		
		JavaPairRDD<Text,IntWritable> result = input.mapToPair(new ConvertToWritableTypes());
		result.saveAsHadoopFile("outputFile", Text.class, IntWritable.class, SequenceFileOutputFormat.class);
	}
}
