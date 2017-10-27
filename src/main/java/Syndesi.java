import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;


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

        StructType schema = new StructType(new StructField[]{
                new StructField("ap1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap4", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("ap5", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
        });

        AbsoluteTransformer absoluter = new AbsoluteTransformer(new String[]{"ap1", "ap2", "ap3", "ap4", "ap5"});


        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"ap1", "ap2", "ap3", "ap4", "ap5"})
                .setOutputCol("features");

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


        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();


        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {absoluter, assembler, nb});

        PipelineModel model = pipeline.fit(trainData);

        Dataset<Row> predictions = model.transform(testData);
        predictions.show();

        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);

        spark.stop();
    }
}
