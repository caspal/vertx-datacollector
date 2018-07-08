package info.pascalkrause.vertx.datacollector;

import java.util.concurrent.TimeUnit;

import info.pascalkrause.vertx.datacollector.job.CollectorJob;
import info.pascalkrause.vertx.datacollector.service.DataCollectorService;
import info.pascalkrause.vertx.datacollector.service.DataCollectorServiceImpl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

public class DataCollectorServiceVerticle extends AbstractVerticle {

    private final String address;
    private final CollectorJob job;
    private final int workerPoolSize;
    private final int queueSize;
    private final boolean enableMetrics;
    private final long maxExecuteTimeout;

    private DataCollectorService dcs;
    private ServiceBinder binder;
    private MessageConsumer<JsonObject> consumer;

    /**
     * @param address The eventbus address
     * @param job The job which will be processed in the CollectorJobExecutor
     * @param workerPoolSize The pool size of the CollectorJobExecutor
     * @param queueSize The queue size of CollectorJob requests
     * @param enableMetrics Enables metrics for the DataCollectorService
     */
    public DataCollectorServiceVerticle(String address, CollectorJob job, int workerPoolSize, int queueSize,
            boolean enableMetrics) {
        this(address, job, workerPoolSize, queueSize, enableMetrics, TimeUnit.MINUTES.toMillis(1));
    }

    /**
     * @param address The eventbus address
     * @param job The job which will be processed in the CollectorJobExecutor
     * @param workerPoolSize The pool size of the CollectorJobExecutor
     * @param queueSize The queue size of CollectorJob requests
     * @param enableMetrics Enables metrics for the DataCollectorService
     * @param maxExecuteTimeout Timeout for a job in the ExecutorPool in milliseconds
     */
    public DataCollectorServiceVerticle(String address, CollectorJob job, int workerPoolSize, int queueSize,
            boolean enableMetrics, long maxExecuteTimeout) {
        this.address = address;
        this.job = job;
        this.workerPoolSize = workerPoolSize;
        this.queueSize = queueSize;
        this.enableMetrics = enableMetrics;
        this.maxExecuteTimeout = maxExecuteTimeout;
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        dcs = new DataCollectorServiceImpl(vertx, job, workerPoolSize, queueSize, enableMetrics, maxExecuteTimeout,
                address);
        binder = new ServiceBinder(vertx);
    }

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        consumer = binder.setAddress(address).register(DataCollectorService.class, dcs);
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        binder.unregister(consumer);
        stopFuture.complete();
    }

    /**
     * @return The address of the eventbus
     */
    public String getServiceAddress() {
        return address;
    }
}
