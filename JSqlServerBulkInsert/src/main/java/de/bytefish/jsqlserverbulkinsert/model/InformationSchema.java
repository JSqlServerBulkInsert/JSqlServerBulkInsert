package de.bytefish.jsqlserverbulkinsert.model;

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
}
