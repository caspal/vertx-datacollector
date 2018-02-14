package info.pascalkrause.vertx.datacollector;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.report.ReportOptions;

public class TestUtils {

    public static final boolean DEBUG_MODE = java.lang.management.ManagementFactory.getRuntimeMXBean()
            .getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

    public static void runTruthTests(TestContext context, Handler<Void> testCode) {
        try {
            testCode.handle(null);
        } catch (final Throwable t) {
            context.fail(t);
        }
    }

    public static TestOptions getTestOptions() {
        final Vertx vertx = Vertx.vertx();
        final String reportsPath = "./testreports";
        if (!vertx.fileSystem().existsBlocking(reportsPath)) {
            vertx.fileSystem().mkdirBlocking(reportsPath);
        }
        final TestOptions opts = new TestOptions().addReporter(new ReportOptions().setTo("console"));
        opts.addReporter(new ReportOptions().setTo("file:./testreports/").setFormat("junit"));
        opts.addReporter(new ReportOptions().setTo("file:./testreports/").setFormat("simple"));
        return opts;
    }
}
