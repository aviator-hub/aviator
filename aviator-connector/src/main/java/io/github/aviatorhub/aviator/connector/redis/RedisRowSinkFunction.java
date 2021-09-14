package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.AviatorRowPartitioner;
import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.KeyExtractor;
import io.github.aviatorhub.aviator.connector.clickhouse.ClickHouseRowFlusher;
import io.github.aviatorhub.aviator.core.AviatorBufferManager;
import io.github.aviatorhub.aviator.core.AviatorFlusher;
import io.github.aviatorhub.aviator.core.AviatorPartitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

public class RedisRowSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

  private final ConnectorConf conf;
  private final KeyExtractor keyExtractor;
  private final RowDataToJsonConverter valueConverter;

  private transient AviatorPartitioner<RowData> partitioner;
  private transient AviatorBufferManager<RowData> bufferManager;
  private transient AviatorFlusher<RowData> flusher;

  public RedisRowSinkFunction(ConnectorConf conf,
      KeyExtractor keyExtractor,
      RowDataToJsonConverter valueConverter) {
    this.conf = conf;
    this.keyExtractor = keyExtractor;
    this.valueConverter = valueConverter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    flusher = new RedisRowFlusher(conf, keyExtractor, valueConverter);
    partitioner = new AviatorRowPartitioner();
    bufferManager = new AviatorBufferManager(conf.getBufferConf(),
        RowData.class, partitioner, flusher);
    bufferManager.init();
    bufferManager.startTimeoutTrigger();
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    super.invoke(value, context);
    bufferManager.add(value);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    bufferManager.checkpoint();
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext)
      throws Exception {

  }
}
