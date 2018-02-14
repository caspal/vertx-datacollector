package info.pascalkrause.vertx.datacollector;

import java.util.function.Function;
import java.util.function.Supplier;

import info.pascalkrause.vertx.datacollector.client.DataCollectorServiceClient;
import info.pascalkrause.vertx.datacollector.client.DataCollectorServiceFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestSuite;

public class DataCollectorTestSuite {

    private final TestSuite suite;
    private final Vertx vertx;
    private final DataCollectorServiceVerticle verticle;
    private DataCollectorServiceClient dcsc;
    private final DeliveryOptions deliveryOptions;

    public DataCollectorTestSuite(String name, DataCollectorServiceVerticle verticle, DeliveryOptions deliveryOptions) {
        suite = TestSuite.create(name);
        vertx = Vertx.vertx();
        this.verticle = verticle;
        this.deliveryOptions = deliveryOptions;
        before();
        after();
    }

    private void before() {
        suite.before(c -> {
            final Async complete = c.async();
            vertx.deployVerticle(verticle, deployResult -> {
                dcsc = new DataCollectorServiceClient(
                        new DataCollectorServiceFactory(vertx, verticle.getServiceAddress()).create(deliveryOptions));
                complete.complete();
            });
        });
    }

    public void addTest(String name, Function<Supplier<DataCollectorServiceClient>, Handler<TestContext>> testCase) {
        suite.test(name, testCase.apply(() -> dcsc));
    }

    private void after() {
        suite.after(c -> {
            final Async complete = c.async();
            vertx.close(closeResult -> complete.complete());
        });
    }

    public TestSuite get() {
        return suite;
    }
}
