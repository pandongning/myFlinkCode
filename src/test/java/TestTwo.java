import flink.mystream.utils.FlinkUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestTwo {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameterTool, SimpleStringSchema.class);

        kafkaStream.map(line -> line.concat("name")).print();

        env.execute();
    }
}

