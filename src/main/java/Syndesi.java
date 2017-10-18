import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Syndesi {
    public static void main(String[] args) {
        String logFile = "/home/carron8/Spark/README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder()
                .config("spark.master", "spark://ubuntu:7077")
                .appName("Syndesi")
                .getOrCreate();
//        Dataset<String> logData = spark.read().textFile(logFile).cache();
//
//        long numAs = logData.filter(s -> s.contains("a")).count();
//        long numBs = logData.filter(s -> s.contains("b")).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
