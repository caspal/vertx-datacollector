package info.pascalkrause.vertx.datacollector;

import static com.google.common.truth.Truth.assertThat;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;

import info.pascalkrause.vertx.datacollector.client.DataCollectorServiceClient;
import info.pascalkrause.vertx.datacollector.client.error.QueueLimitReached;
import info.pascalkrause.vertx.datacollector.job.CollectorJob;
import info.pascalkrause.vertx.datacollector.job.CollectorJobResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestCompletion;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DataCollectorServiceTestSuiteRunner {

    private static final String serviceAddress = "myService";
    private static final int workerPoolSize = 2;

    private static final CollectorJob job = new TestJob();
    private final JsonObject feature = new JsonObject().put("feature-key", "feature-value");

    @Test
    public void testDataCollectorService(TestContext testContext) {
        final Async testContextComplete = testContext.async();
        final DataCollectorServiceVerticle verticle = new DataCollectorServiceVerticle(serviceAddress, job,
                workerPoolSize, workerPoolSize, false);
        final DeliveryOptions delOpts = new DeliveryOptions()
                .setSendTimeout(TimeUnit.SECONDS.toMillis(TestUtils.DEBUG_MODE ? 999999 : 1));
        final DataCollectorTestSuite testSute = new DataCollectorTestSuite("DataCollectorService", verticle, delOpts);

        testSute.addTest("performCollectionAndGetResult", getPerformCollectionAndGetResultTest());
        testSute.addTest("performCollectionAndGetResultWithError", getPerformCollectionAndGetResultWithErrorTest());
        testSute.addTest("performCollection", getPerformCollectionTest());
        testSute.addTest("responseQueueFull", getResponseQueueFullTest());
        testSute.addTest("queueAcceptsJobsAgainAfterFull", getQueueAcceptsJobsAgainAfterFullTest());
        testSute.addTest("getMetricsSnapshot", getMetricsSnapshotTest());

        final TestCompletion tc = testSute.get().run(Vertx.vertx(), TestUtils.getTestOptions());
        if (TestUtils.DEBUG_MODE) {
            tc.awaitSuccess();
        } else {
            tc.awaitSuccess(TimeUnit.SECONDS.toMillis(2));
        }
        testContextComplete.complete();
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getPerformCollectionAndGetResultTest() {
        return dcs -> {
            final String requestId = UUID.randomUUID().toString();
            final CollectorJobResult expectedResult = new CollectorJobResult(requestId, "test-source", "test-quality",
                    "test-created", new JsonObject(), null);
            return c -> {
                final Async testComplete = c.async();
                dcs.get().performCollectionAndGetResult(requestId, feature, res -> {
                    TestUtils.runTruthTests(c, v -> {
                        assertThat(res.succeeded()).isTrue();
                        assertThat(res.result()).isEqualTo(expectedResult);
                    });
                    testComplete.complete();
                });
            };
        };
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getPerformCollectionAndGetResultWithErrorTest() {
        return dcs -> {
            final String requestId = UUID.randomUUID().toString();
            final String errorName = "myError";
            final CollectorJobResult expectedResult = new CollectorJobResult(requestId, "test-source", "test-quality",
                    "test-created", new JsonObject(), new CollectorJobResult.Error(errorName));
            return c -> {
                final Async testComplete = c.async();
                dcs.get().performCollectionAndGetResult(requestId,
                        feature.copy().put(TestJob.KEY_ERROR_NAME, errorName), res -> {
                            TestUtils.runTruthTests(c, v -> {
                                assertThat(res.succeeded()).isTrue();
                                assertThat(res.result()).isEqualTo(expectedResult);
                            });
                            testComplete.complete();
                        });
            };
        };
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getPerformCollectionTest() {
        return dcs -> {
            final String requestId = UUID.randomUUID().toString();
            return c -> {
                final Async testComplete = c.async();
                dcs.get().performCollection(requestId, feature, res -> {
                    TestUtils.runTruthTests(c, v -> assertThat(res.succeeded()).isTrue());
                    testComplete.complete();
                });
            };
        };
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getResponseQueueFullTest() {
        return dcs -> {
            return c -> {
                final Async testComplete = c.async(3);
                IntStream.of(1, 2, 3).mapToObj(i -> i + "").forEach(requestId -> {
                    dcs.get().performCollectionAndGetResult(requestId, feature.copy().put(TestJob.KEY_SLEEP, 10),
                            res -> {
                                if (res.succeeded()) {
                                    TestUtils.runTruthTests(c,
                                            v -> assertThat(res.result().getRequestId()).isAnyOf("1", "2"));
                                } else {
                                    TestUtils.runTruthTests(c,
                                            v -> assertThat(res.cause()).isInstanceOf(QueueLimitReached.class));
                                }
                                testComplete.countDown();
                                if (!testComplete.isCompleted() && (testComplete.count() == 0)) {
                                    testComplete.complete();
                                }
                            });
                });
            };
        };
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getQueueAcceptsJobsAgainAfterFullTest() {
        return dcs -> {
            return c -> {
                final Async testComplete = c.async();
                IntStream.of(1, 2, 3).mapToObj(i -> i + "").forEach(requestId -> {
                    dcs.get().performCollectionAndGetResult(requestId, feature.copy().put(TestJob.KEY_SLEEP, 10),
                            res -> {
                                if (res.failed()) {
                                    TestUtils.runTruthTests(c,
                                            v -> assertThat(res.cause()).isInstanceOf(QueueLimitReached.class));
                                    try {
                                        Thread.sleep(20);
                                        dcs.get().performCollectionAndGetResult(requestId, feature, res2 -> {
                                            TestUtils.runTruthTests(c, v -> assertThat(res2.succeeded()).isTrue());
                                            testComplete.complete();
                                        });
                                    } catch (final InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                });
            };
        };
    }

    private Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> getMetricsSnapshotTest() {
        return dcs -> {
            return c -> {
                final Async testComplete = c.async();
                dcs.get().getMetricsSnapshot(res -> {
                    TestUtils.runTruthTests(c, v -> {
                        assertThat(res.succeeded()).isTrue();
                        assertThat(res.result()).isEqualTo(new JsonObject().put("Error", "Metrics are not enabled"));
                    });
                    testComplete.complete();
                });
            };
        };
    }
}
