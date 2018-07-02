package info.pascalkrause.vertx.datacollector.service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.MetricRegistry;

import info.pascalkrause.vertx.datacollector.job.CollectorJob;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import info.pascalkrause.vertx.datacollector.metrics.MetricSnapshotFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;

public class DataCollectorServiceImpl implements DataCollectorService {

    private final WorkerExecutor collectorJobExecutor;
    private final WorkerExecutor postCollectExecutor;
    private final CollectorJob collectorJob;
    private final int queueSize;
    private final AtomicInteger currentQueueSize = new AtomicInteger(0);
    private final MetricSnapshotFactory metricFactory;

    public DataCollectorServiceImpl(Vertx vertx, CollectorJob job, int workerPoolSize, int queueSize,
            boolean enableMetrics, long maxExecuteTimeout) {
        collectorJobExecutor = vertx.createSharedWorkerExecutor("CollectorJobExecutor-Pool", workerPoolSize,
                TimeUnit.MILLISECONDS.toNanos(maxExecuteTimeout));
        postCollectExecutor = vertx.createSharedWorkerExecutor("PostCollectExecutor-Pool", workerPoolSize,
                TimeUnit.MILLISECONDS.toNanos(maxExecuteTimeout));
        collectorJob = job;
        this.queueSize = queueSize;
        if (enableMetrics) {
            metricFactory = new MetricSnapshotFactory(new MetricRegistry());
            metricFactory.registerQueueMetrics(currentQueueSize, queueSize);
        } else {
            metricFactory = null;
        }
    }

    @Override
    public void collectAndReceive(String requestId, JsonObject feature,
            Handler<AsyncResult<CollectorJobResult>> resultHandler) {
        if (currentQueueSize.intValue() < queueSize) {
            currentQueueSize.incrementAndGet();
            collectorJobExecutor.executeBlocking(collectorJob.collect(requestId, feature), false, collectResult -> {
                postCollectExecutor.executeBlocking(collectorJob.postCollectAction(collectResult), false,
                        postResult -> {
                            currentQueueSize.decrementAndGet();
                            if (Objects.nonNull(metricFactory)) {
                                metricFactory.registerTotalMetrics(postResult);
                            }
                            resultHandler.handle(postResult);
                        });
            });
        } else {
            resultHandler.handle(Future.failedFuture(ERROR_QUEUE_LIMIT_REACHED));
        }
    }

    @Override
    public void collect(String requestId, JsonObject feature, Handler<AsyncResult<Void>> resultHandler) {
        collectAndReceive(requestId, feature, res -> {
            resultHandler.handle(res.failed() ? Future.failedFuture(res.cause()) : Future.succeededFuture());
        });
    }

    /**
     * Visible for Testing
     */
    public JsonObject getMetricsSnapshot() {
        return Objects.isNull(metricFactory) ? new JsonObject().put("Error", "Metrics are not enabled")
                : metricFactory.getMetricsSnapshot();
    }

    @Override
    public void getMetricsSnapshot(Handler<AsyncResult<JsonObject>> resultHandler) {
        resultHandler.handle(Future.succeededFuture(getMetricsSnapshot()));
    }

    @Override
    public void close() {
        // Needed for generated Client
    }
}
