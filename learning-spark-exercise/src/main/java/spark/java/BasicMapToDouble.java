package spark.java;

import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;

public class BasicMapToDouble {

	public static void main(String[] args) throws Exception {
		String master;
		if (args.length > 0) {
			master = args[0];
		} else {
			master = "local";
		}

		JavaSparkContext sc = new JavaSparkContext(master, "basicMapToDouble",
				System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		
		JavaDoubleRDD result = rdd.mapToDouble(new DoubleFunction<Integer>() {
			
			@Override
			public double call(Integer x) throws Exception {
				return (double) x * x;
			}
		});
		
		System.out.println(result.mean());
	}

}
