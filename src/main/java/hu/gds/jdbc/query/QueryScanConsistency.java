package hu.gds.jdbc.query;

public enum QueryScanConsistency {
    NOT_BOUNDED {
        public String toString() {
            return "not_bounded";
        }
    },
    REQUEST_PLUS {
        public String toString() {
            return "request_plus";
        }
    };

    private QueryScanConsistency() {
    }
}
