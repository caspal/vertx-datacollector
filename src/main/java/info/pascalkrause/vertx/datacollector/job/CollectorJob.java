package info.pascalkrause.vertx.datacollector.job;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 * A generic interface which must be implemented to run the collection job inside the CollectorJobExecutor worker pool.
 */
public interface CollectorJob {

    /**
     * This method should be used to create a Future that contains the collection logic. The Future will be executed in
     * a worker thread pool, which allows blocking operations inside the Future.
     *
     * @param requestId A request id to identify the collection request.
     * @param feature A JSON object to pass attributes and properties which are needed for the collection process.
     * @return A Handler with the Future which contains the collection logic.
     */
    public Handler<Future<CollectorJobResult>> collect(String requestId, JsonObject feature);

    /**
     * This method will be called after the {@link #collect(String, JsonObject)} method and returns a Future which can
     * be used to do some post-collect stuff like rough parsing or saving the result into a database. The Future will be
     * executed in a worker thread pool, which allows blocking operations inside the Future.
     *
     * @param result The {@link CollectorJobResult} from the previous called {@link #collect(String, JsonObject)}
     * method.
     * @return A Handler with the Future which contains the post-collection logic.
     */
    public Handler<Future<CollectorJobResult>> postCollectAction(AsyncResult<CollectorJobResult> result);
}
