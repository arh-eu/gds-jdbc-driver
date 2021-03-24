package hu.gds.jdbc.resultset;

public class ResultSetWrapper {
    private final AbstractGdsResultSet resultSet;
    private final boolean isNull;

    public ResultSetWrapper(AbstractGdsResultSet resultSet, boolean isNull) {
        this.resultSet = resultSet;
        this.isNull = isNull;
    }

    public AbstractGdsResultSet getResultSet() {
        return resultSet;
    }

    public boolean isNull() {
        return isNull;
    }
}
