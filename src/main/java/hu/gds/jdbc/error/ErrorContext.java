package hu.gds.jdbc.error;

import hu.gds.jdbc.msg.ResponseStatus;
import hu.gds.jdbc.annotation.Stability;
import hu.gds.jdbc.cnc.AbstractContext;

import java.util.Map;

/**
 * The ErrorContext is the parent interface for all service-specific error contexts that are thrown as part of
 * the {@link GdsException}.
 */
@Stability.Uncommitted
public abstract class ErrorContext extends AbstractContext {

    private final ResponseStatus responseStatus;

    protected ErrorContext(final ResponseStatus responseStatus) {
        this.responseStatus = responseStatus;
    }

    public ResponseStatus responseStatus() {
        return responseStatus;
    }

    @Override
    public void injectExportableParams(final Map<String, Object> input) {
        super.injectExportableParams(input);
        if (responseStatus != null) {
            input.put("status", responseStatus);
        }
    }

}
