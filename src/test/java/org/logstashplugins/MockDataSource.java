package org.logstashplugins;



import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.logging.Logger;

import static org.mockito.Mockito.*;

/**
 * Created by: Safak T. at 5/11/2020
 * While listening: F.W.T.B. - grandson Remix - YONAKA @Link https://open.spotify.com/track/7GENPjwC2JICc2oCe2N2Xm
 */
class MockDataSource implements DataSource {

    private Exception exceptionForBatchExecute = null;
    private Exception exceptionForExecuteUpdate = null;
    
    public MockDataSource() {
    }

    public MockDataSource(Exception throwThisAtExecuteBatchAndExecuteUpdate) {
        exceptionForBatchExecute = throwThisAtExecuteBatchAndExecuteUpdate;
        exceptionForExecuteUpdate = throwThisAtExecuteBatchAndExecuteUpdate;
    }

    public MockDataSource(Exception exceptionForBatchExecute, Exception exceptionForExecuteUpdate) {
        this.exceptionForBatchExecute = exceptionForBatchExecute;
        this.exceptionForExecuteUpdate = exceptionForExecuteUpdate;
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        return createMockConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public Connection createMockConnection() throws SQLException {
        // Setup mock connection
        final Connection mockConnection = mock(Connection.class);

        // Autocommit is always true by default
        when(mockConnection.getAutoCommit()).thenReturn(true);

        // Handle Connection.createStatement()
        Statement statement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(statement);
        when(mockConnection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(mockConnection.createStatement(anyInt(), anyInt(), anyInt())).thenReturn(statement);
        when(mockConnection.isValid(anyInt())).thenReturn(true);

        // Handle Connection.prepareStatement()
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), (int[]) any())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), (String[]) any())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockPreparedStatement);
        doAnswer((Answer<Void>) invocation -> null).doNothing().when(mockPreparedStatement).setInt(anyInt(), anyInt());

        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        if (exceptionForExecuteUpdate != null){
            when(mockPreparedStatement.executeUpdate()).thenThrow(exceptionForExecuteUpdate);   
               
        }else{
            when(mockPreparedStatement.executeUpdate()).thenReturn(0);
        }

        
        if (exceptionForBatchExecute != null){
            when(mockPreparedStatement.executeBatch()).thenThrow(exceptionForBatchExecute);    
        }else {
            when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{0,0});   
        }
        
        when(mockResultSet.getString(anyInt())).thenReturn("aString");
        when(mockResultSet.next()).thenReturn(true);

        // Handle Connection.prepareCall()
        CallableStatement mockCallableStatement = mock(CallableStatement.class);
        when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);
        when(mockConnection.prepareCall(anyString(), anyInt(), anyInt())).thenReturn(mockCallableStatement);
        when(mockConnection.prepareCall(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockCallableStatement);

        ResultSet mockResultSetTypeInfo = mock(ResultSet.class);

        DatabaseMetaData mockDataBaseMetadata = mock(DatabaseMetaData.class);
        when(mockDataBaseMetadata.getDatabaseProductName()).thenReturn("PostgreSQL");
        when(mockDataBaseMetadata.getDatabaseMajorVersion()).thenReturn(8);
        when(mockDataBaseMetadata.getDatabaseMinorVersion()).thenReturn(2);
        when(mockDataBaseMetadata.getConnection()).thenReturn(mockConnection);
        when(mockDataBaseMetadata.getTypeInfo()).thenReturn(mockResultSetTypeInfo);
        when(mockConnection.getMetaData()).thenReturn(mockDataBaseMetadata);


        // Handle Connection.close()
        doAnswer((Answer<Void>) invocation -> null).doThrow(new SQLException("Connection is already closed")).when(mockConnection).close();

        // Handle Connection.commit()
        doAnswer((Answer<Void>) invocation -> null).doThrow(new SQLException("Transaction already committed")).when(mockConnection).commit();

        // Handle Connection.rollback()
        doAnswer((Answer<Void>) invocation -> null).doThrow(new SQLException("Transaction already rolled back")).when(mockConnection).rollback();

        return mockConnection;
    }
    
}
