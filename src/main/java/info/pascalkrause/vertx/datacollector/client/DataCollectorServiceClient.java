package info.pascalkrause.vertx.datacollector.client;

import info.pascalkrause.vertx.datacollector.client.error.QueueLimitReached;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import info.pascalkrause.vertx.datacollector.service.DataCollectorService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class DataCollectorServiceClient implements DataCollectorService {

    private final DataCollectorService dcs;

    public DataCollectorServiceClient(DataCollectorService dcs) {
        this.dcs = dcs;
    }

    @SuppressWarnings("unchecked")
    private <E> AsyncResult<E> checkForError(AsyncResult<?> res) {
        if (res.failed() && ERROR_QUEUE_LIMIT_REACHED.equals(res.cause().getMessage())) {
            return Future.failedFuture(new QueueLimitReached());
        }
        return (AsyncResult<E>) res;
    }

    @Override
    public void performCollectionAndGetResult(String requestId, JsonObject feature,
            Handler<AsyncResult<CollectorJobResult>> resultHandler) {
        dcs.performCollectionAndGetResult(requestId, feature, res -> resultHandler.handle(checkForError(res)));
    }

    @Override
    public void performCollection(String requestId, JsonObject feature, Handler<AsyncResult<Void>> resultHandler) {
        dcs.performCollection(requestId, feature, res -> resultHandler.handle(checkForError(res)));
    }

    @Override
    public void getMetricsSnapshot(Handler<AsyncResult<JsonObject>> resultHandler) {
        dcs.getMetricsSnapshot(resultHandler);
    }

    @Override
    public void close() {
        dcs.close();
    }
}
