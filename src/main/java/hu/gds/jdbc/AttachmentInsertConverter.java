package hu.gds.jdbc;

import hu.arheu.gds.message.data.MessageData6AttachmentResponse;
import hu.arheu.gds.message.data.impl.AttachmentResultHolderImpl;
import hu.arheu.gds.message.util.MessageManager;
import hu.arheu.gds.message.util.ValidationException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.insert.Insert;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttachmentInsertConverter {
    private static final String META_FIELD_NAME = "meta";
    private static final String TTL_FIELD_NAME = "ttl";
    private static final String OWNER_ID_FIELD_NAME = "ownerid";
    private static final String ATTACHMENT_DATA_FIELD_NAME = "data";
    private static final String ATTACHMENT_ID_FIELD_NAME = "id";

    public static MessageData6AttachmentResponse getMessageData6AttachmentResponse(String sql)
            throws SQLException, IOException, ValidationException, JSQLParserException {

        Statements statements = CCJSqlParserUtil.parseStatements(sql);
        net.sf.jsqlparser.statement.Statement statement = statements.getStatements().get(0);
        Insert insert = (Insert) statement;

        String tableName = getTableName(insert);
        String meta = null;
        Long ttl;
        String ownerId = null;
        byte[] attachment;
        String attachmentId = null;

        ItemsList itemsList = insert.getItemsList();
        ExpressionList expressionList = (ExpressionList) itemsList;
        List<Expression> expressions = expressionList.getExpressions();

        Map<String, Integer> columnIndexMapping = getColumnIndexMapping(insert);

        Integer metaFieldIndex = columnIndexMapping.get(META_FIELD_NAME);
        if (metaFieldIndex != null) {
            meta = ((StringValue) expressions.get(metaFieldIndex)).getValue();
        }
        Integer ttlFieldIndex = columnIndexMapping.get(TTL_FIELD_NAME);
        if (ttlFieldIndex != null) {
            ttl = ((LongValue) expressions.get(ttlFieldIndex)).getValue();
        } else {
            throw new SQLException("Column 'ttl' is a mandatory field and it not found in the orphan attachment insert.", insert.toString(), -1);
        }
        Integer ownerIdFieldIndex = columnIndexMapping.get(OWNER_ID_FIELD_NAME);
        if (ownerIdFieldIndex != null) {
            ownerId = ((StringValue) expressions.get(ownerIdFieldIndex)).getValue();
        } else {
            throw new SQLException("Column 'ownerid' is a mandatory field and it not found in the orphan attachment insert.", insert.toString(), -1);
        }
        Integer attachmentDataFieldIndex = columnIndexMapping.get(ATTACHMENT_DATA_FIELD_NAME);
        if (attachmentDataFieldIndex != null) {
            attachment = GdsBaseStatement.hexStringToByteArray(
                    ((HexValue) expressions.get(attachmentDataFieldIndex)).getValue().substring(2));
        } else {
            throw new SQLException("Column 'data' is a mandatory field and it not found in the orphan attachment insert.", insert.toString(), -1);
        }
        Integer attachmentIdFieldIndex = columnIndexMapping.get(ATTACHMENT_ID_FIELD_NAME);
        if (attachmentIdFieldIndex != null) {
            attachmentId = ((StringValue) expressions.get(attachmentIdFieldIndex)).getValue();
        } else {
            throw new SQLException("Column 'id' is a mandatory field and it not found in the orphan attachment insert.", insert.toString(), -1);
        }

        List<String> requestIds = new ArrayList<>();
        requestIds.add(attachmentId);
        List<String> ownerIds = new ArrayList<>();
        ownerIds.add(ownerId);
        AttachmentResultHolderImpl attachmentResultHolder = new AttachmentResultHolderImpl(
                requestIds,
                tableName,
                attachmentId,
                ownerIds,
                meta,
                ttl,
                null,
                attachment);
        return MessageManager.createMessageData6AttachmentResponse(attachmentResultHolder, null);
    }

    private static Map<String, Integer> getColumnIndexMapping(Insert insert) throws SQLException {
        Map<String, Integer> columnIndexMapping = new HashMap<>();
        List<Column> columns = insert.getColumns();
        if (null == columns || columns.isEmpty()) {
            throw new SQLException("There is no columns in attachment insert", insert.toString(), -1);
        }
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getColumnName();
            switch (columnName) {
                case META_FIELD_NAME:
                    columnIndexMapping.put(META_FIELD_NAME, i);
                    break;
                case TTL_FIELD_NAME:
                    columnIndexMapping.put(TTL_FIELD_NAME, i);
                    break;
                case OWNER_ID_FIELD_NAME:
                    columnIndexMapping.put(OWNER_ID_FIELD_NAME, i);
                    break;
                case ATTACHMENT_DATA_FIELD_NAME:
                    columnIndexMapping.put(ATTACHMENT_DATA_FIELD_NAME, i);
                    break;
                case ATTACHMENT_ID_FIELD_NAME:
                    columnIndexMapping.put(ATTACHMENT_ID_FIELD_NAME, i);
                    break;
                default:
                    throw new SQLException("Unknown column name found in orphan attachment insert: " + columnName, insert.toString(), -1);
            }
        }
        return columnIndexMapping;
    }

    private static String getTableName(Insert insert) {
        Table table = insert.getTable();
        if (table != null) {
            String ownerTable = table.getName().replace("-@attachment", "");
            return ownerTable.replace("\"", "");
        }
        return null;
    }
}
