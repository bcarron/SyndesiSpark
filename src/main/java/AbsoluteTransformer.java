import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.abs;
import org.apache.spark.sql.types.StructType;

/**
 * Transform a raw dataset to one with column names
 */
public class AbsoluteTransformer extends Transformer {
    private static final long serialVersionUID = -627462023364910959L;
    private String[] columns;

    AbsoluteTransformer(String[] columns) {
        this.columns = columns;
    }

    @Override
    public String uid() {
        return "AbsoluteTransformer" + serialVersionUID;
    }

    @Override
    public Transformer copy(ParamMap arg0) {
        return null;
    }

    @Override
    public Dataset transform(Dataset data) {
        for (String col: columns){
            data = data.withColumn(col, abs(data.col(col)));
        }
        return data;
    }

    @Override
    public StructType transformSchema(StructType arg0) {
        return arg0;
    }
}