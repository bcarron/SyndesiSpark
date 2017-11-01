import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;

public class SVMCLassifier {
    private Pipeline pipeline;
    private PipelineModel model;

    public SVMCLassifier() {
        // create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();


        this.pipeline = new Pipeline()
                .setStages(new PipelineStage[] {DataTransformer.getAbsoluter(), DataTransformer.getAssembler(), nb});
    }
}
