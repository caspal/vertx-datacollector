package info.pascalkrause.vertx.datacollector.metrics;

import static com.google.common.truth.Truth.assertThat;
import static info.pascalkrause.vertx.datacollector.TestJob.FEATURE_ERROR;
import static info.pascalkrause.vertx.datacollector.TestJob.FEATURE_HANDLED_EXCEPTION;
import static info.pascalkrause.vertx.datacollector.TestJob.FEATURE_STOP;
import static info.pascalkrause.vertx.datacollector.TestJob.FEATURE_SUCCEEDED;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import info.pascalkrause.vertx.datacollector.TestJob;
import info.pascalkrause.vertx.datacollector.TestUtils;
import info.pascalkrause.vertx.datacollector.service.DataCollectorServiceImpl;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)

public class MetricsTest {

    private DataCollectorServiceImpl classUnderTest;
    private Vertx vertx;
    private final long defaultExecutorTimeout = TimeUnit.MINUTES.toMillis(1);

    private JsonObject buildExpectedMetricsObject(int METRIC_TOTAL_JOBS_COUNT, int METRIC_TOTAL_JOBS_FAILED,
            int METRIC_TOTAL_JOBS_SUCCEEDED, int METRIC_TOTAL_JOBS_EXCEPTION, int METRIC_QUEUE_MAX_SIZE,
            int METRIC_QUEUE_FREE, int METRIC_QUEUE_OCCUPIED, JsonObject quality, JsonObject errors) {

        final JsonObject total = new JsonObject();
        total.put("jobs", new JsonObject().put("count", METRIC_TOTAL_JOBS_COUNT).put("failed", METRIC_TOTAL_JOBS_FAILED)
                .put("succeeded", METRIC_TOTAL_JOBS_SUCCEEDED).put("exception", METRIC_TOTAL_JOBS_EXCEPTION));
        total.put("quality", quality);
        total.put("errors", errors);

        final JsonObject queue = new JsonObject();
        queue.put("maxSize", METRIC_QUEUE_MAX_SIZE).put("free", METRIC_QUEUE_FREE).put("occupied",
                METRIC_QUEUE_OCCUPIED);

        final JsonObject metrics = new JsonObject();
        metrics.put("total", total);
        metrics.put("queue", queue);

        return metrics;
    }

    @Before
    public void beforeEach(TestContext c) {
        vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(10));
    }

    @After
    public void afterEach(TestContext c) {
        vertx.close(c.asyncAssertSuccess());
    }

    @Test
    public void testNoMetricss() {
        classUnderTest = new DataCollectorServiceImpl(vertx, new TestJob(), 2, 10, false, defaultExecutorTimeout);
        assertThat(classUnderTest.getMetricsSnapshot())
                .isEqualTo(new JsonObject().put("Error", "Metrics are not enabled"));
    }

    @Test
    public void testNoMetricsAndEmptyMetrics() {
        classUnderTest = new DataCollectorServiceImpl(vertx, new TestJob(), 10, 30, true, defaultExecutorTimeout);
        assertThat(classUnderTest.getMetricsSnapshot())
                .isEqualTo(buildExpectedMetricsObject(0, 0, 0, 0, 30, 30, 0, new JsonObject(), new JsonObject()));
    }

    @Test
    public void testTotalMetrics(TestContext c) throws InterruptedException {
        final Async a = c.async(17);
        final String reqId = "myRequest";
        final AtomicInteger count = new AtomicInteger(0);
        classUnderTest = new DataCollectorServiceImpl(vertx, new TestJob(), 20, 30, true, defaultExecutorTimeout);
        IntStream.range(0, 2).forEach(i -> {
            classUnderTest.collect(reqId, FEATURE_ERROR, (v) -> count.incrementAndGet());
            classUnderTest.collect(reqId, new JsonObject().put(TestJob.KEY_ERROR_NAME, "someError2"),
                    (v) -> count.incrementAndGet());
            a.countDown();
        });
        IntStream.range(0, 1).forEach(i -> {
            classUnderTest.collect(reqId, new JsonObject().put(TestJob.KEY_ERROR_NAME, "someError3"),
                    (v) -> count.incrementAndGet());
            classUnderTest.collect(reqId, new JsonObject().put(TestJob.KEY_ERROR_NAME, "someError4"),
                    (v) -> count.incrementAndGet());
            a.countDown();
        });
        IntStream.range(0, 10).forEach(i -> {
            classUnderTest.collect(reqId, FEATURE_SUCCEEDED, (v) -> count.incrementAndGet());
            a.countDown();
        });
        IntStream.range(0, 4).forEach(i -> {
            classUnderTest.collect(reqId, FEATURE_HANDLED_EXCEPTION, (v) -> count.incrementAndGet());
            a.countDown();
        });
        a.await(TimeUnit.SECONDS.toMillis(TestUtils.DEBUG_MODE ? 999999 : 1));

        final JsonObject errors = new JsonObject().put("someError", 2).put("someError2", 2).put("someError3", 1)
                .put("someError4", 1);

        TimeUnit.MILLISECONDS.sleep(250); // Wait for a short moment, until metrics are finally written
        assertThat(classUnderTest.getMetricsSnapshot()).isEqualTo(buildExpectedMetricsObject(count.get(), 6, 10, 4, 30,
                30, 0, new JsonObject().put("test-quality", 10), errors));
    }

    @Test
    public void testQueueMetrics(TestContext c) {
        final Async a = c.async();
        final String reqId = "myRequest";
        final AtomicInteger count = new AtomicInteger(0);
        classUnderTest = new DataCollectorServiceImpl(vertx, new TestJob(a), 10, 30, true, defaultExecutorTimeout);
        classUnderTest.collect(reqId, FEATURE_STOP, (v) -> count.incrementAndGet());
        classUnderTest.collect(reqId, FEATURE_STOP, (v) -> count.incrementAndGet());
        classUnderTest.collect(reqId, FEATURE_STOP, (v) -> count.incrementAndGet());

        assertThat(classUnderTest.getMetricsSnapshot())
                .isEqualTo(buildExpectedMetricsObject(0, 0, 0, 0, 30, 27, 3, new JsonObject(), new JsonObject()));
        a.complete();
    }

    @Test
    public void testSortDescendingAndSlice() {
        final Map<String, AtomicLong> unsorted = new HashMap<>();
        unsorted.put("1", new AtomicLong(1));
        unsorted.put("2", new AtomicLong(2));
        unsorted.put("3", new AtomicLong(3));
        unsorted.put("4", new AtomicLong(4));

        final Map<String, Long> sorted = new HashMap<>();
        sorted.put("3", 3L);
        sorted.put("4", 4L);

        assertThat(MetricSnapshotFactory.sortDescendingAndSlice(unsorted, 2)).containsExactlyEntriesIn(sorted);
    }
}
