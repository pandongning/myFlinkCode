package flink.mystream.asyncIo.asyncmysql;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author pdn
 */
public class AsyncQueryFromMySQL {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> stream2 = AsyncDataStream.unorderedWait(stream, new AsyncMySQLRequest(), 0, TimeUnit.MILLISECONDS);

        stream2.print();

        env.execute();
    }
}
