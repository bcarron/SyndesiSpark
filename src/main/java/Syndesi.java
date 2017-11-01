import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Syndesi {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Syndesi Spark")
                .config("spark.master", "local") //spark://ubuntu:7077
                .getOrCreate();

/*        Dataset<Row> data = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://129.194.71.126/dataset")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "measurements")
                .option("user", "spark")
                .option("password", "spark").load();*/

        NaiveBayesClassifier nb = new NaiveBayesClassifier();

        StructType schema = new StructType(new StructField[]{
                new StructField("ap1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap4", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap5", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
        });

        Dataset<Row> trainData = spark.read()
                .schema(schema)
                .option("header", "false")
                .option("delimiter", "\t")
                .csv("data/rssData.txt");



        Dataset<Row> testData = spark.read()
                .schema(schema)
                .option("header", "false")
                .option("delimiter", "\t")
                .csv("data/testData.txt");


        nb.train(trainData);

        nb.classify(testData);

        spark.stop();
    }
}
