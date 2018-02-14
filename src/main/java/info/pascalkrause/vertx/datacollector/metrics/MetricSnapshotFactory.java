package info.pascalkrause.vertx.datacollector.metrics;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult.Error;
import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonObject;

public class MetricSnapshotFactory {

    public static final String METRIC_QUEUE_MAX_SIZE = "QueueMaxSize";
    public static final String METRIC_QUEUE_FREE = "QueueFree";
    public static final String METRIC_QUEUE_OCCUPIED = "QueueOccupied";

    public static final String METRIC_TOTAL_JOBS_COUNT = "totalJobsCount";
    private final Counter totalJobsCounter;
    public static final String METRIC_TOTAL_JOBS_FAILED = "totalJobsFailed";
    private final Counter totalJobsFailed;
    public static final String METRIC_TOTAL_JOBS_SUCCEEDED = "totalJobsSucceeded";
    private final Counter totalJobsSucceeded;
    public static final String METRIC_TOTAL_JOBS_EXCEPTION = "totalJobsException";
    private final Counter totalJobsException;

    private final MetricRegistry metricRegistry;

    Map<String, AtomicLong> qualityMap = new ConcurrentHashMap<>();
    Map<String, AtomicLong> errorMap = new ConcurrentHashMap<>();

    private Map<String, Object> sortDescendingAndSlice(Map<String, AtomicLong> unsorted, long maxEntries) {
        return unsorted.entrySet().stream().map(e -> new SimpleEntry<String, Long>(e.getKey(), e.getValue().get()))
                .sorted(Map.Entry.comparingByValue()).limit(maxEntries).collect(Collectors.toMap(e -> e.getKey(),
                        e -> e.getValue(), (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    private void addOrIncrease(Map<String, AtomicLong> map, String key) {
        if (map.containsKey(key)) {
            map.get(key).incrementAndGet();
        } else {
            map.put(key, new AtomicLong(1));
        }
    }

    public void addQueueMetrics(AtomicInteger currentQueueSize, int queueSize) {
        metricRegistry.register(MetricRegistry.name(METRIC_QUEUE_MAX_SIZE), (Gauge<Integer>) () -> queueSize);
        metricRegistry.register(MetricRegistry.name(METRIC_QUEUE_FREE),
                (Gauge<Integer>) () -> queueSize - currentQueueSize.get());
        metricRegistry.register(MetricRegistry.name(METRIC_QUEUE_OCCUPIED),
                (Gauge<Integer>) () -> currentQueueSize.get());
    }

    public void addTotalMetricsCounters(AsyncResult<CollectorJobResult> postResult) {
        totalJobsCounter.inc();
        if (postResult.succeeded()) {
            final Optional<Error> e = postResult.result().getError();
            if (e.isPresent()) {
                totalJobsFailed.inc();
                addOrIncrease(errorMap, e.get().getName());
            } else {
                totalJobsSucceeded.inc();
                addOrIncrease(qualityMap, postResult.result().getQuality());
            }
        } else {
            totalJobsException.inc();
        }
    }

    public MetricSnapshotFactory(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        totalJobsCounter = metricRegistry.counter(METRIC_TOTAL_JOBS_COUNT);
        totalJobsFailed = metricRegistry.counter(METRIC_TOTAL_JOBS_FAILED);
        totalJobsSucceeded = metricRegistry.counter(METRIC_TOTAL_JOBS_SUCCEEDED);
        totalJobsException = metricRegistry.counter(METRIC_TOTAL_JOBS_EXCEPTION);
    }

    private JsonObject getQueueMetrics() {
        final JsonObject queue = new JsonObject();
        metricRegistry.getGauges(MetricFilter.contains("Queue")).forEach((name, metric) -> {
            switch (name) {
            case METRIC_QUEUE_MAX_SIZE:
                queue.put("maxSize", metric.getValue());
                break;
            case METRIC_QUEUE_FREE:
                queue.put("free", metric.getValue());
                break;
            case METRIC_QUEUE_OCCUPIED:
                queue.put("occupied", metric.getValue());
                break;

            default:
                throw new IllegalStateException("Unknown metric: " + name);
            }
        });
        return queue;
    }

    private JsonObject getTotalJobsMetrics() {
        final JsonObject jobs = new JsonObject();
        metricRegistry.getCounters(MetricFilter.startsWith("totalJobs")).forEach((name, metric) -> {
            switch (name) {
            case METRIC_TOTAL_JOBS_COUNT:
                jobs.put("count", metric.getCount());
                break;
            case METRIC_TOTAL_JOBS_FAILED:
                jobs.put("failed", metric.getCount());
                break;
            case METRIC_TOTAL_JOBS_SUCCEEDED:
                jobs.put("succeeded", metric.getCount());
                break;
            case METRIC_TOTAL_JOBS_EXCEPTION:
                jobs.put("exception", metric.getCount());
                break;

            default:
                throw new IllegalStateException("Unknown metric: " + name);
            }
        });
        return jobs;
    }

    public JsonObject getMetricsSnapshot() {
        final JsonObject total = new JsonObject();
        total.put("jobs", getTotalJobsMetrics());
        total.put("quality", new JsonObject(sortDescendingAndSlice(qualityMap, 10)));
        total.put("errors", new JsonObject(sortDescendingAndSlice(errorMap, 10)));

        final JsonObject metrics = new JsonObject();
        metrics.put("total", total);
        metrics.put("queue", getQueueMetrics());
        return metrics;
    }
}
