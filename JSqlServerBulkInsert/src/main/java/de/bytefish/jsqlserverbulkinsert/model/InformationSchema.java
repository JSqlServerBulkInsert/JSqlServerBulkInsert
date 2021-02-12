package de.bytefish.jsqlserverbulkinsert.model;

import java.util.List;

public class InformationSchema {

    /**
     * The Column Information with the Column Name and associated Oridnal:
     */
    public static class ColumnInformation {

        private final String columnName;
        private final int ordinal;

        public ColumnInformation(String columnName, int ordinal) {
            this.columnName = columnName;
            this.ordinal = ordinal;
        }

        public String getColumnName() {
            return columnName;
        }

        public int getOrdinal() {
            return ordinal;
        }
    }

    private final List<ColumnInformation> columns;

    public InformationSchema(List<ColumnInformation> columns) {
        this.columns = columns;
    }

    public List<ColumnInformation> getColumns() {
        return columns;
    }
}
