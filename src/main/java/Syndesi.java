import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Perform the Syndesi localization algorithm
 */
public class Syndesi {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Syndesi Spark")
                .config("spark.master", "spark://129.194.246.176:7077") //spark://ubuntu:7077 spark://129.194.246.176:7077
                .getOrCreate();

        // Actual DB connection
/*        Dataset<Row> data = spark.read().format("jdbc")
=======
        // Read last location from database
        Dataset<Row> data = spark.read().format("jdbc")
>>>>>>> c4df825220c0e6affead3d7875d4c4b02bb471df
                .option("url", "jdbc:mysql://129.194.71.126/dataset")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "measurements")
                .option("user", "spark")
                .option("password", "spark").load();

        // Create the classifier
        NaiveBayesClassifier nb = new NaiveBayesClassifier();

        // Data schema
        StructType schema = new StructType(new StructField[]{
                new StructField("ap1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap4", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap5", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
        });

        // Train data
        Dataset<Row> trainData = spark.read()
                .schema(schema)
                .option("header", "false")
                .option("delimiter", "\t")
                .csv("data/rssData.txt");


        // Simulate DB connection for test data
        Dataset<Row> testData = spark.read()
                .schema(schema)
                .option("header", "false")
                .option("delimiter", "\t")
                .csv("data/testData.txt");


        // Train the classifier
        nb.train(trainData);

        // Classify the new RSSI vectors
        nb.classify(testData);

        spark.stop();
    }
}
