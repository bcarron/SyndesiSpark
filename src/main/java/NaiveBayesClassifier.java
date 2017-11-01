import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class NaiveBayesClassifier {
    private Pipeline pipeline;
    private PipelineModel model;


    public NaiveBayesClassifier() {
        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();


        this.pipeline = new Pipeline()
                .setStages(new PipelineStage[] {DataTransformer.getAbsoluter(), DataTransformer.getAssembler(), nb});
    }

    public void train(Dataset trainData) {
        this.model = this.pipeline.fit(trainData);
    }

    public Dataset<Row> classify(Dataset testData) {
        Dataset<Row> predictions = model.transform(testData);
        predictions.show();

        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);

        return predictions;
    }
}
