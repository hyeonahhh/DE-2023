import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
public final class UBERStudent20191012 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERtudent20191012 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20191012")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

	JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) {
			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			String[] sent = s.split(",");
			String region = sent[0];
			String[] dlist = sent[1].split("/");
			String month = dlist[0];
			String day = dlist[1];
			String year = dlist[2];
			LocalDate date = LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day));
			DayOfWeek dow = date.getDayOfWeek();
			int dowNum = dow.getValue();
			String d = days[dowNum - 1];
			
			String key = region + "," + d;
			String value = sent[3] + "," + sent[2];
			return new Tuple2(key, value);
		}
	});
        
        JavaPairRDD<String, String> counts = words.reduceByKey(new Function2<String, String, String>() {
        	public String call(String x, String y) {
        		String[] x_li = x.split(",");
        		String[] y_li = y.split(",");
        		
        		int trips = Integer.parseInt(x_li[0]) + Integer.parseInt(y_li[0]);
        		int vehicles = Integer.parseInt(x_li[1]) + Integer.parseInt(y_li[1]);
        		return trips + "," + vehicles;
        	}
        });
        
        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
