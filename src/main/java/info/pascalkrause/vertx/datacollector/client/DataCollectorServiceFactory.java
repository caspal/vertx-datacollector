package info.pascalkrause.vertx.datacollector.client;

import info.pascalkrause.vertx.datacollector.service.DataCollectorService;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.serviceproxy.ServiceProxyBuilder;

public class DataCollectorServiceFactory {

    private final ServiceProxyBuilder builder;

    public DataCollectorServiceFactory(Vertx vertx, String serviceAddress) {
        builder = new ServiceProxyBuilder(vertx).setAddress(serviceAddress);
    }

    public DataCollectorService create() {
        return builder.build(DataCollectorService.class);
    }

    public DataCollectorService create(DeliveryOptions options) {
        return builder.setOptions(options).build(DataCollectorService.class);
    }
}
