package hu.gds.jdbc.cnc;

import java.util.Map;
import java.util.TreeMap;

/**
 * Common parent method for all contexts.
 *
 * <p>Contexts are encouraged to derive from this abstract class because all they have
 * to do then is to implement/override {@link #injectExportableParams(Map)} and feed
 * the data they want to be extracted. The actual extraction and formatting then
 * comes for free.</p>
 */
public abstract class AbstractContext implements Context {

    /**
     * This method needs to be implemented by the actual context implementations to
     * inject the params they need for exporting.
     *
     * @param input pass exportable params in here.
     */
    public void injectExportableParams(final Map<String, Object> input) {
    }

    @Override
    public String exportAsString(final ExportFormat format) {
        Map<String, Object> input = new TreeMap<>();
        injectExportableParams(input);
        return format.apply(input);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + exportAsString(ExportFormat.STRING);
    }
}
