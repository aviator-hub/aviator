package io.github.aviatorhub.aviator.connector.clickhouse;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import java.util.List;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import io.github.aviatorhub.aviator.connector.AviatorRowPartitioner;
import io.github.aviatorhub.aviator.core.AviatorBufferManager;
import io.github.aviatorhub.aviator.core.AviatorFlusher;
import io.github.aviatorhub.aviator.core.AviatorPartitioner;

/**
 * ClickHouse Sink Function.
 *
 * @author meijie
 */
public class ClickHouseRowSinkFunction extends RichSinkFunction<RowData> implements
    CheckpointedFunction {

  private final ConnectorConf conf;
  private final List<Column> columns;
  private final Optional<UniqueConstraint> primaryKey;

  private transient AviatorPartitioner<RowData> partitioner;
  private transient AviatorBufferManager<RowData> bufferManager;
  private transient AviatorFlusher<RowData> flusher;

  public ClickHouseRowSinkFunction(ConnectorConf conf, ResolvedSchema schema) {
    this.conf = conf;
    this.columns = schema.getColumns();
    this.primaryKey = schema.getPrimaryKey();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    flusher = new ClickHouseRowFlusher(conf, columns);
    partitioner = new AviatorRowPartitioner();
    bufferManager = new AviatorBufferManager(conf.getBufferConf(),
        RowData.class, partitioner, flusher);
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
