package info.pascalkrause.vertx.datacollector.service;

import info.pascalkrause.vertx.datacollector.DataCollectorServiceVerticle;
import info.pascalkrause.vertx.datacollector.job.CollectorJob;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import io.vertx.codegen.annotations.ProxyClose;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface DataCollectorService {

    public static final String ERROR_QUEUE_LIMIT_REACHED = "queueLimitReached";

    /**
     * This method triggers the start of the {@link CollectorJob} in the {@link DataCollectorServiceVerticle} and
     * receives the result of the CollectorJob. If the queue of the DataCollectorServiceVerticle is already full, the
     * response will be a failed AsyncResult with the message which is speified in
     * {@link DataCollectorService#ERROR_QUEUE_LIMIT_REACHED}.
     *
     * @param requestId A request id to identify the collection request.
     * @param feature A JSON object to pass attributes and properties which are needed for the collection process.
     * @param resultHandler A handler to process the result.
     */
    public void performCollectionAndGetResult(String requestId, JsonObject feature,
            Handler<AsyncResult<CollectorJobResult>> resultHandler);

    /**
     * This method is doing the same as {@link #performCollectionAndGetResult(String, JsonObject, Handler)}, but will
     * not send back the result. It will just send back the information if the job succeeded or failed.
     *
     * @param requestId A request id to identify the collection request.
     * @param feature A JSON object to pass attributes and properties which are needed for the collection process.
     * @param resultHandler A handler to process the result.
     */
    public void performCollection(String requestId, JsonObject feature, Handler<AsyncResult<Void>> resultHandler);

    /**
     * Returns a JsonObject which contains a current snapshot of the metrics. The JsonObject has more or less the
     * following structure:
     *
     * <pre>
     * {
     *   total: {
     *     jobs: {
     *       count: 123,
     *       failed: 12,
     *       succeeded: 108,
     *       exception: 3
     *     },
     *     quality: {
     *       complete: 90,
     *       partial: 20,
     *       ....
     *     },
     *     errors: {
     *       blocked: 90,
     *       timeout: 20,
     *       ....
     *     }
     *   },
     *   queue: {
     *     maxSize: 30,
     *     free: 12,
     *     occupied: 18
     *   }
     * }
     * </pre>
     * <p>
     *
     * @param resultHandler A handler to process the metrics result.
     */
    public void getMetricsSnapshot(Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Is needed for the proxy generation
     */
    @ProxyClose
    void close();
}
