package info.pascalkrause.vertx.datacollector;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import info.pascalkrause.vertx.datacollector.job.CollectorJob;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;

public class TestJob implements CollectorJob {

    public static final String KEY_SLEEP = "sleep";
    public static final String KEY_ERROR_NAME = "errorName";
    public static final String KEY_HANDLED_EXCEPTION = "handledException";
    public static final String KEY_UNHANDLED_EXCEPTION = "unhandledException";
    public static final String KEY_STOP = "stop";

    public static final JsonObject FEATURE_ERROR = new JsonObject().put(KEY_ERROR_NAME, "someError");
    public static final JsonObject FEATURE_HANDLED_EXCEPTION = new JsonObject().put(KEY_HANDLED_EXCEPTION,
            "someHandledError");
    public static final JsonObject FEATURE_UNHANDLED_EXCEPTION = new JsonObject().put(KEY_UNHANDLED_EXCEPTION,
            "someUnhandledError");
    public static final JsonObject FEATURE_SUCCEEDED = new JsonObject();
    public static final JsonObject FEATURE_STOP = new JsonObject().put(KEY_STOP, "stop");

    private final Async stopper;

    public TestJob() {
        this(null);
    }

    public TestJob(Async stopper) {
        this.stopper = stopper;
    }

    @Override
    public Handler<Future<CollectorJobResult>> postCollectAction(AsyncResult<CollectorJobResult> result) {
        return fut -> {
            if (result.failed()) {
                fut.fail(result.cause());
            } else {
                fut.complete(result.result());
            }
        };
    }

    private CollectorJobResult generateResult(String requestId) {
        return generateResult(requestId, null);
    }

    private CollectorJobResult generateResult(String requestId, CollectorJobResult.Error error) {
        return new CollectorJobResult(requestId, "test-src", "test-quality", "test-created", new JsonObject(), error);
    }

    @Override
    public Handler<Future<CollectorJobResult>> collect(String requestId, JsonObject feature) {
        CollectorJobResult jobResult;
        if (feature.containsKey(KEY_ERROR_NAME)) {
            jobResult = generateResult(requestId, new CollectorJobResult.Error(feature.getString(KEY_ERROR_NAME)));
        } else {
            jobResult = generateResult(requestId);
        }

        return fut -> {
            final Long sleepTime = feature.getLong(KEY_SLEEP);
            if (Objects.nonNull(sleepTime)) {
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (Objects.nonNull(stopper) && feature.containsKey(KEY_STOP)) {
                stopper.await(TimeUnit.SECONDS.toMillis(1));
            }

            if (feature.containsKey(KEY_UNHANDLED_EXCEPTION)) {
                throw new RuntimeException("Some unhandled excpetion");
            } else if (feature.containsKey(KEY_HANDLED_EXCEPTION)) {
                fut.fail(new RuntimeException("Some handled exception"));
            } else {
                fut.complete(jobResult);
            }
        };
    }
}
