import org.apache.spark.ml.feature.VectorAssembler;

public final class DataTransformer {
    public static AbsoluteTransformer getAbsoluter() {
        return new AbsoluteTransformer(new String[]{"ap1", "ap2", "ap3", "ap4", "ap5"});
    }


    public static VectorAssembler getAssembler() {
        return new VectorAssembler()
                .setInputCols(new String[]{"ap1", "ap2", "ap3", "ap4", "ap5"})
                .setOutputCol("features");
    }
}
