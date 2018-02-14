package info.pascalkrause.vertx.datacollector.job;

import java.util.Objects;
import java.util.Optional;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject
public class CollectorJobResult {

    /**
     * The class {@link CollectorJobResult.Error} can be used to store all error related information which occur during
     * the processing of the {@link CollectorJob} implementation. This class extends JsonObject and adds the method
     * {@link #getName()} to ensure that every error object has a name, which is used for the metrics.
     *
     */
    public static class Error extends JsonObject {
        private static final String KEY_NAME = "_name";

        public static Optional<Error> fromJson(JsonObject error) {
            if (Objects.isNull(error) || Objects.isNull(error.getString(KEY_NAME))) {
                return Optional.ofNullable(null);
            }
            final Error newError = new Error(error.getString(KEY_NAME));
            error.mergeIn(newError);
            return Optional.of(newError);
        }

        public Error(String name) {
            super();
            this.put(KEY_NAME, name);
        }

        public String getName() {
            return this.getString(KEY_NAME);
        }
    }

    private static final String KEY_REQUEST_ID = "requestId";
    private static final String KEY_SOURCE = "source";
    private static final String KEY_QUALITY = "quality";
    private static final String KEY_CREATED = "created";
    private static final String KEY_RESULT = "result";
    private static final String KEY_ERROR = "error";

    private final JsonObject data = new JsonObject();

    /**
     * Is used by the generated DataCollectorService proxy.
     *
     * @param o A JsonObject which contains all fields to create a {@link CollectorJobResult}.
     */
    public CollectorJobResult(JsonObject o) {
        data.put(KEY_REQUEST_ID, o.getString(KEY_REQUEST_ID)).put(KEY_SOURCE, o.getString(KEY_SOURCE))
                .put(KEY_QUALITY, o.getString(KEY_QUALITY)).put(KEY_CREATED, o.getString(KEY_CREATED))
                .put(KEY_RESULT, o.getJsonObject(KEY_RESULT)).put(KEY_ERROR, o.getJsonObject(KEY_ERROR));
    }

    /**
     * This class stores the information which will be collected in the {@link CollectorJob} implementation.
     *
     * @param requestId The requestId from the CollectorJobRequest.
     * @param source The source of the collected information.
     * @param quality A String that express the quality of the collected data (e.g. complete, partial, ..).
     * @param created A String which indicates when the result was created (e.g. in ISO 8601 format).
     * @param result A JsonObject which contains the result of the {@link CollectorJob}.
     * @param error An Error object that stores all relevant information if an error occurs.
     */
    public CollectorJobResult(String requestId, String source, String quality, String created, JsonObject result,
            Error error) {
        super();
        data.put(KEY_REQUEST_ID, requestId).put(KEY_SOURCE, source).put(KEY_QUALITY, quality).put(KEY_CREATED, created)
                .put(KEY_RESULT, result).put(KEY_ERROR, error);
    }

    public String getRequestId() {
        return data.getString(KEY_REQUEST_ID);
    }

    public String getSource() {
        return data.getString(KEY_SOURCE);
    }

    public String getQuality() {
        return data.getString(KEY_QUALITY);
    }

    public String getCreated() {
        return data.getString(KEY_CREATED);
    }

    public JsonObject getResult() {
        return data.getJsonObject(KEY_RESULT);
    }

    public Optional<Error> getError() {
        return Error.fromJson(data.getJsonObject(KEY_ERROR));
    }

    public JsonObject toJson() {
        return data;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof CollectorJobResult) && (hashCode() == obj.hashCode())) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
