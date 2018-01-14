import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;

/**
 * SVM multiclass classifier
 */
public class SVMCLassifier {
    private Pipeline pipeline;
    private PipelineModel model;

    public SVMCLassifier() {
        // create the trainer and set its parameters
        LinearSVC svm = new LinearSVC();


        this.pipeline = new Pipeline()
                .setStages(new PipelineStage[] {DataTransformer.getAbsoluter(), DataTransformer.getAssembler(), svm});
    }
}
