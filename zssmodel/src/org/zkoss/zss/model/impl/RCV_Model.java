package org.zkoss.zss.model.impl;

import com.opencsv.CSVReader;
import org.apache.tomcat.dbcp.dbcp2.DelegatingConnection;
import org.model.AutoRollbackConnection;
import org.model.BlockStore;
import org.model.DBContext;
import org.model.DBHandler;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.jdbc.PgConnection;
import org.zkoss.zss.model.CellRegion;
import org.zkoss.zss.model.SSheet;

import java.io.IOException;
import java.io.Reader;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.IntStream;


public class RCV_Model extends Model {
    private Logger logger = Logger.getLogger(RCV_Model.class.getName());
    protected PosMapping rowMapping;
    protected PosMapping colMapping;
    private BlockStore bs;
    private MetaDataBlock metaDataBlock;

    //Create or load RCV_model.
    protected RCV_Model(DBContext context, SSheet sheet, String tableName) {
        this.sheet = sheet;
        rowMapping = new BTree(context, tableName + "_row_idx");
        colMapping = new BTree(context, tableName + "_col_idx");
        this.tableName = tableName;
        createSchema(context);
        loadMetaData(context);
    }


    private void loadMetaData(DBContext context) {
        bs = new BlockStore(context, tableName + "_rcv_meta");
        metaDataBlock = bs.getObject(context, 0, MetaDataBlock.class);
        if (metaDataBlock == null) {
            metaDataBlock = new MetaDataBlock();
            bs.putObject(0, metaDataBlock);
            bs.flushDirtyBlocks(context);
        }
    }


    //Create a table from the database
    private void createSchema(DBContext dbContext) {
        String createTable = (new StringBuffer())
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append("(row INT, col INT, data BYTEA)")
                .toString();
        String createIndex = (new StringBuffer())
                .append("CREATE INDEX IF NOT EXISTS ")
                .append(tableName)
                .append("_row_col ON ")
                .append(tableName)
                .append("(row, col)")
                .toString();
        AutoRollbackConnection connection = dbContext.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTable);
            stmt.execute(createIndex);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dropSchema(DBContext context) {
        String dropTable = (new StringBuffer())
                .append("DROP TABLE ")
                .append(tableName)
                .toString();
        AutoRollbackConnection connection = context.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(dropTable);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        bs.dropSchemaAndClear(context);
        rowMapping.dropSchema(context);
        colMapping.dropSchema(context);
    }


    @Override
    public void insertRows(DBContext context, int row, int count) {
        rowMapping.createIDs(context, row, count);
    }

    @Override
    public void insertCols(DBContext context, int col, int count) {
        colMapping.createIDs(context, col, count);
    }

    @Override
    public void deleteRows(DBContext dbContext, int row, int count) {
        Integer[] ids = rowMapping.deleteIDs(dbContext, row, count);

        AutoRollbackConnection connection = dbContext.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(
                "DELETE FROM " + tableName + " WHERE row = ANY(?)")) {
            Array inArray = dbContext.getConnection().createArrayOf("integer", ids);
            stmt.setArray(1, inArray);
            stmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteCols(DBContext context, int col, int count) {
        Integer[] ids = colMapping.deleteIDs(context, col, count);

        metaDataBlock.deletedColumns.addAll(Arrays.asList(ids));
        // simplified conversion
        /*
        for (int id : ids)
            metaDataBlock.deletedColumns.add(id);
        */

        bs.putObject(0, metaDataBlock);
        bs.flushDirtyBlocks(context);

        /* Do delete lazy
        try (PreparedStatement stmt = context.getConnection().prepareStatement(
                "DELETE FROM " + tableName + " WHERE col = ANY(?)")) {
            Array inArray = context.getConnection().createArrayOf("integer", ids);
            stmt.setArray(1, inArray);
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } */
    }

    public void executeLazyDelete(DBContext dbContext) {
        Integer[] ids = (Integer[]) metaDataBlock.deletedColumns.toArray();

        AutoRollbackConnection connection = dbContext.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(
                "DELETE FROM " + tableName + " WHERE col = ANY(?)")) {
            Array inArray = connection.createArrayOf("integer", ids);
            stmt.setArray(1, inArray);
            stmt.execute();
            metaDataBlock.deletedColumns.clear();
            bs.putObject(0, metaDataBlock);
            bs.flushDirtyBlocks(dbContext);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void updateCells(DBContext context, Collection<AbstractCellAdv> cells) {

        StringBuffer update = new StringBuffer("WITH upsert AS ( UPDATE ")
                .append(tableName)
                .append(" SET data = ? WHERE row = ? AND col = ? RETURNING *) INSERT INTO ")
                .append(tableName)
                .append(" (row,col,data) SELECT ?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)");

        AutoRollbackConnection connection = context.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(update.toString())) {
            for (AbstractCellAdv cell : cells) {
                // Extend sheet
                Integer[] idsRow = rowMapping.getIDs(context, cell.getRowIndex(), 1);
                int row = idsRow[0];
                Integer[] idsCol = colMapping.getIDs(context, cell.getColumnIndex(), 1);
                int col = idsCol[0];
                stmt.setBytes(1, cell.toBytes());
                stmt.setInt(2, row);
                stmt.setInt(3, col);
                stmt.setInt(4, row);
                stmt.setInt(5, col);
                stmt.setBytes(6, cell.toBytes());
                stmt.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteCells(DBContext dbContext, CellRegion range) {

        Integer[] rowIds = rowMapping.getIDs(dbContext, range.getRow(), range.getLastRow() - range.getRow() + 1);
        Integer[] colIds = colMapping.getIDs(dbContext, range.getColumn(), range.getLastColumn() - range.getColumn() + 1);

        String delete = new StringBuffer("DELETE FROM ")
                .append(tableName)
                .append(" WHERE row = ANY (?) AND col = ANY (?)").toString();


        AutoRollbackConnection connection = dbContext.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(delete)) {

            Array inArrayRow = dbContext.getConnection().createArrayOf("integer", rowIds);
            stmt.setArray(1, inArrayRow);

            Array inArrayCol = dbContext.getConnection().createArrayOf("integer", colIds);
            stmt.setArray(2, inArrayCol);

            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void deleteCells(DBContext dbContext, Collection<AbstractCellAdv> cells) {

        String delete = new StringBuffer("DELETE FROM ")
                .append(tableName)
                .append(" WHERE row = ? AND col = ?").toString();

        AutoRollbackConnection connection = dbContext.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(delete)) {
            for (AbstractCellAdv cell : cells) {
                Integer[] idsRow = rowMapping.getIDs(dbContext, cell.getRowIndex(), 1);
                int row = idsRow[0];
                Integer[] idsCol = colMapping.getIDs(dbContext, cell.getColumnIndex(), 1);
                int col = idsCol[0];
                stmt.setObject(1, row);
                stmt.setObject(2, col);
                stmt.execute();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean deleteTableRows(DBContext context, CellRegion cellRegion) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<AbstractCellAdv> getCells(DBContext context, CellRegion fetchRange) {
        // Reduce Range to bounds
        Collection<AbstractCellAdv> cells = new ArrayList<>();

        CellRegion bounds =  getBounds(context);
        if (bounds==null || fetchRange==null)
            return cells;

        CellRegion fetchRegion = bounds.getOverlap(fetchRange);
        if (fetchRegion == null)
            return cells;

        Integer[] rowIds = rowMapping.getIDs(context, fetchRegion.getRow(), fetchRegion.getLastRow() - fetchRegion.getRow() + 1);
        Integer[] colIds = colMapping.getIDs(context, fetchRegion.getColumn(), fetchRegion.getLastColumn() - fetchRegion.getColumn() + 1);
        HashMap<Integer, Integer> row_map = IntStream.range(0, rowIds.length)
                .collect(HashMap<Integer, Integer>::new, (map, i) -> map.put(rowIds[i], fetchRegion.getRow() + i), null);

        HashMap<Integer, Integer> col_map = IntStream.range(0, colIds.length)
                .collect(HashMap<Integer, Integer>::new, (map, i) -> map.put(colIds[i], fetchRegion.getColumn() + i), null);


        String select = new StringBuffer("SELECT row, col, data FROM ")
                .append(tableName)
                .append(" WHERE row = ANY (?) AND col = ANY (?)").toString();


        AutoRollbackConnection connection = context.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(select)) {

            Array inArrayRow = context.getConnection().createArrayOf("integer", rowIds);
            stmt.setArray(1, inArrayRow);

            Array inArrayCol = context.getConnection().createArrayOf("integer", colIds);
            stmt.setArray(2, inArrayCol);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                int row_id = rs.getInt(1);
                int col_id = rs.getInt(2);
                AbstractCellAdv cell = CellImpl.fromBytes(sheet, row_map.get(row_id),
                        col_map.get(col_id), rs.getBytes(3));
                cells.add(cell);
            }
            rs.close();
            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cells;
    }

    @Override
    public CellRegion getBounds(DBContext context) {
        int rows = rowMapping.size(context);
        int columns = colMapping.size(context);
        if (rows==0 || columns ==0)
            return null;
        else
            return new CellRegion(0, 0, rowMapping.size(context) - 1, colMapping.size(context) - 1);
    }

    @Override
    public void clearCache(DBContext context) {
        rowMapping.clearCache(context);
        colMapping.clearCache(context);
    }

    @Override
    public void importSheet(Reader reader, char delimiter) throws IOException {
        final int COMMIT_SIZE_BYTES = 8 * 1000;
        CSVReader csvReader = new CSVReader(reader, delimiter);
        String[] nextLine;
        int importedRows = 0;
        int importedColumns = 0;


        try (AutoRollbackConnection connection = DBHandler.instance.getConnection()) {
            Connection rawConn = ((DelegatingConnection) connection.getInternalConnection()).getInnermostDelegate();
            CopyManager cm = ((PgConnection) rawConn).getCopyAPI();

            CopyIn cpIN = cm.copyIn("COPY " + tableName + " (row,col,data)" +
                    " FROM STDIN WITH DELIMITER '|'");

            StringBuffer sb = new StringBuffer();
            while ((nextLine = csvReader.readNext()) != null) {
                ++importedRows;
                if (importedColumns < nextLine.length)
                    importedColumns = nextLine.length;
                for (int col = 0; col < nextLine.length; col++) {
                    sb.append(importedRows).append('|');
                    sb.append(col+1).append('|');
                    sb.append(nextLine[col]).append('\n');
                }

                if (sb.length() >= COMMIT_SIZE_BYTES) {
                    cpIN.writeToCopy(sb.toString().getBytes(), 0, sb.length());
                    sb = new StringBuffer();
                }
            }
            if (sb.length() > 0)
                cpIN.writeToCopy(sb.toString().getBytes(), 0, sb.length());
            cpIN.endCopy();
            rawConn.commit();
            DBContext dbContext = new DBContext(connection);
            insertRows(dbContext, 0, importedRows);
            insertCols(dbContext, 0, importedColumns);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean deleteTableColumns(DBContext dbContext, CellRegion cellRegion) {
        throw new UnsupportedOperationException();
    }

    private static class MetaDataBlock {
        List<Integer> deletedColumns;

        MetaDataBlock() {
            deletedColumns = new ArrayList<>();
        }
    }

    @Override
    public void updateRowSize(DBContext context, int row, int height){
        StringBuffer update = new StringBuffer("WITH upsert AS ( UPDATE ")
                .append(tableName)
                .append(" SET data = ? WHERE row = ? AND col = ? RETURNING *) INSERT INTO ")
                .append(tableName)
                .append(" (row,col,data) SELECT ?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)");

        AutoRollbackConnection connection = context.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(update.toString())) {

                stmt.setInt(1, Integer.valueOf(height).byteValue());
                stmt.setInt(2, row);
                stmt.setInt(3, -1);
                stmt.setInt(4, row);
                stmt.setInt(5, -1);
                stmt.setInt(6, Integer.valueOf(height).byteValue());
                stmt.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateColSize(DBContext context, int col, int width) {

        StringBuffer update = new StringBuffer("WITH upsert AS ( UPDATE ")
                .append(tableName)
                .append(" SET data = ? WHERE row = ? AND col = ? RETURNING *) INSERT INTO ")
                .append(tableName)
                .append(" (row,col,data) SELECT ?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)");

        AutoRollbackConnection connection = context.getConnection();
        try (PreparedStatement stmt = connection.prepareStatement(update.toString())) {
            stmt.setInt(1, Integer.valueOf(width).byteValue());
            stmt.setInt(2, -1);
            stmt.setInt(3, col);
            stmt.setInt(4, -1);
            stmt.setInt(5, col);
            stmt.setInt(6, Integer.valueOf(width).byteValue());
            stmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
