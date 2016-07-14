package spark.java;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BasicLoadJson {

	public static class Person implements java.io.Serializable {
		public String name;
		public Boolean lovesPandas;
	}
	
	public static class ParseJson implements FlatMapFunction<Iterator<String>,Person>{

		public Iterable<Person> call(Iterator<String> lines) throws Exception {
			ArrayList<Person> people = new ArrayList<Person>();
			ObjectMapper mapper = new ObjectMapper();
			while(lines.hasNext()){
				String line = lines.next();
				try{
					people.add(mapper.readValue(line, Person.class));
				}catch (Exception e) {
					// skip records on failure
				}
			}
			return people;
		}
		
	}
	
	public static class WriteJson implements FlatMapFunction<Iterator<Person>,String>{

		@Override
		public Iterable<String> call(Iterator<Person> people) throws Exception {
			ArrayList<String> text = new ArrayList<String>();
			ObjectMapper mapper = new ObjectMapper();
			while(people.hasNext()){
				Person person = people.next();
				text.add(mapper.writeValueAsString(person));
			}
			return text;
		}
		
	}
	
	public static class LikesPandas implements Function<Person,Boolean>{

		@Override
		public Boolean call(Person person) throws Exception {
			return person.lovesPandas;
		}
		
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			throw new Exception(
					"Usage BasicLoadJson [sparkMaster] [jsoninput] [jsonoutput]");
		}
		String master = args[0];
		String fileName = args[1];
		String outfile = args[2];

		JavaSparkContext sc = new JavaSparkContext(master, "basicloadjson",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));

		JavaRDD<String> input = sc.textFile(fileName);
		JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikesPandas());
		
		JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
		formatted.saveAsTextFile(outfile);
	}

}
