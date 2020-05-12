package org.logstashplugins;

import co.elastic.logstash.api.*;
import com.sun.istack.internal.Nullable;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;


@LogstashPlugin(name = "logstash_jdbc_output_plugin")
public class LogstashJdbcOutputPlugin implements Output {
    
    private Logger logger;
    
    public static final PluginConfigSpec<String> CONNECTION_STRING =
            PluginConfigSpec.stringSetting("connectionString", "");
    
    public static final PluginConfigSpec<String> DB_USERNAME =
            PluginConfigSpec.stringSetting("username", "");

    public static final PluginConfigSpec<String> DB_PASSWORD =
            PluginConfigSpec.stringSetting("password", "");
    
    public static final PluginConfigSpec<String> SQL_STATEMENT =
            PluginConfigSpec.stringSetting("sqlStatement", "");
    
    public static final PluginConfigSpec<List<Object>> ORDERED_EVENT_PARAMETER_NAMES =
            PluginConfigSpec.arraySetting("orderedEventParameterNames");
    
    public static final PluginConfigSpec<Long> MAX_POOL_SIZE =
            PluginConfigSpec.numSetting("maxPoolSize", 10);

    public static final PluginConfigSpec<Long> MIN_IDLE =
            PluginConfigSpec.numSetting("minIdle", 10);
    
    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private final SQLExceptionStrategyDecider sqlExceptionStrategyDecider;
    private volatile boolean stopped = false;
    private final int maxWaitInSeconds =  600;
    
    
    private String sqlStatement;
    private List<String> orderedEventParameterNames;
    
    @Nullable private DeadLetterQueueWriter deadLetterQueueWriter;
    private DataSource dataSource;

    private Optional<DeadLetterQueueWriter> getDeadLetterQueueWriter(){
        return Optional.ofNullable(deadLetterQueueWriter);
    }

    public DataSource getDataSource() {
        return dataSource;
    }
    
    public void setDataSource(DataSource dataSource){
        this.dataSource = dataSource;
    }

    public LogstashJdbcOutputPlugin(final String id, final Configuration config, final Context context) {
        deadLetterQueueWriter = context.getDlqWriter();
        logger = context.getLogger(this);
        this.id = id;
        String connectionString = config.get(CONNECTION_STRING);
        String username = config.get(DB_USERNAME);
        String password = config.get(DB_PASSWORD);
        
        sqlStatement = config.get(SQL_STATEMENT);
        orderedEventParameterNames = config.get(ORDERED_EVENT_PARAMETER_NAMES)
                .stream().map(o -> (String)o).collect(Collectors.toList());
        
        int maxPoolSize = config.get(MAX_POOL_SIZE).intValue();
        int minIdleConnection = config.get(MIN_IDLE).intValue();
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(connectionString);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setAllowPoolSuspension(true);
        hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMinimumIdle(minIdleConnection);
        logger.info("Initializing HikariPool");
        dataSource = new HikariDataSource( hikariConfig );

        try {
            String dbName = dataSource.getConnection().getMetaData().getDatabaseProductName();
            sqlExceptionStrategyDecider = new SQLExceptionStrategyDecider(dbName,logger);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not acquire connection to investigate metadata of db.", e);
        }
        
        
        logger.info("Initialized HikariPool");
    }

    @Override
    public void output(final Collection<Event> events) {
        long startTime = System.currentTimeMillis();
        logger.debug(()->"Number of events: "+events.size()+" ");
        try {
            batchCommitUntilUnrepeatable(events);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        logger.debug(()->"Executed: "+events.size()+" in "+(System.currentTimeMillis() - startTime)+" milliseconds.");
    }
    
    private void batchCommitUntilUnrepeatable(Collection<Event> events) throws InterruptedException {
        int waitInSeconds = 1;
        while (true){
            try {
                batchCommit(events);
                return;
            } catch (SQLException e) {
                SQLExceptionResolveStrategy strategy = sqlExceptionStrategyDecider.decide(e);
                if (strategy.equals(SQLExceptionResolveStrategy.RETRY)){
                    logger.error("Retriable SQL Exception occurred will retry again in {} seconds.", waitInSeconds);
                    logger.error("",e);
                    waitWhileNotStopped(waitInSeconds);
                    waitInSeconds = waitInSeconds * 2;
                    waitInSeconds = waitInSeconds > maxWaitInSeconds ? maxWaitInSeconds : waitInSeconds;
                }else if(strategy.equals(SQLExceptionResolveStrategy.SHUTDOWN)){
                    logger.error("A non-retriable SQL Exception occurred, shutting down logstash.",e);
                    throw new IllegalStateException(e);
                }else if (strategy.equals(SQLExceptionResolveStrategy.DISCARD)){
                    logger.error("An SQL Exception occurred, some of the events are corrupt. Will try to insert one-by-one.",e);
                    break;
                }
            } catch (Throwable throwable){
                throw new IllegalStateException(throwable);
            }
        }
        // if this line is reached, try one-by-one
        // reset wait in seconds
        waitInSeconds = 1;
        while (true){
            try (
                    Connection connection = dataSource.getConnection();
            )
            {
                connection.setAutoCommit(false);
                executeOneByOne(connection,events);
                connection.commit();
                break;
            } catch (SQLException e) {
                logger.error("",e);
                waitWhileNotStopped(waitInSeconds);
                waitInSeconds = waitInSeconds * 2;
                waitInSeconds = waitInSeconds > maxWaitInSeconds ? maxWaitInSeconds : waitInSeconds;
            }
        }

    }
    
    private void executeOneByOne(Connection connection, Collection<Event> events) throws SQLException, InterruptedException {
        Savepoint savepoint = null;
        int waitInSeconds = 1;
        Queue<Event> eventQueue = new LinkedList<>(events);
        while (!eventQueue.isEmpty()){
            Event event = eventQueue.peek();
            try {
                //if (savepoint != null) connection.releaseSavepoint(savepoint);
                savepoint = connection.setSavepoint();
                singleInsert(connection, event);
                eventQueue.poll();
                waitInSeconds = 1;
            } catch (SQLException e) {
                logger.error("SQL Exception occurred.",e);
                if (!connection.isClosed()) connection.rollback(savepoint);
                SQLExceptionResolveStrategy strategy = sqlExceptionStrategyDecider.decide(e);
                if (strategy.equals(SQLExceptionResolveStrategy.RETRY)){
                    logger.error("Retriable SQL Exception occurred will retry again in {} seconds.", waitInSeconds);
                    waitWhileNotStopped(waitInSeconds);
                    waitInSeconds = waitInSeconds * 2;
                    waitInSeconds = waitInSeconds > maxWaitInSeconds ? maxWaitInSeconds : waitInSeconds;
                }else if(strategy.equals(SQLExceptionResolveStrategy.SHUTDOWN)){
                    if (!connection.isClosed()) connection.rollback();
                    logger.error("A non-retriable SQL Exception occurred, shutting down logstash.");
                    throw new IllegalStateException(e);
                }else if (strategy.equals(SQLExceptionResolveStrategy.DISCARD)){
                    logger.error("An SQL Exception occurred, event is corrupted. Will add to DLQ if configured.");
                    getDeadLetterQueueWriter().ifPresent(
                            dlq -> {
                                try {
                                    dlq.writeEntry(event,this,"Corrupted event.");
                                    eventQueue.poll();
                                } catch (IOException ex) {
                                    logger.error("DLQ IOException occured, dropping the event.",ex);
                                    eventQueue.poll();
                                }
                            }
                    );
                    waitInSeconds = 1;
                }
            }catch (Throwable throwable){
                logger.error("",throwable);
                if (!connection.isClosed()) connection.rollback();
                throw new IllegalStateException(throwable);
            }
        }
    }
    
    private void waitWhileNotStopped(int waitInSeconds) throws InterruptedException {
        while (waitInSeconds > 0){
            if (!stopped){
                Thread.sleep(1000);
                waitInSeconds--;   
            }else{
                logger.warn("Stop signal occurred, closing down the pipeline {} .",id);
                throw new IllegalStateException();
            }
        }
    }
    
    private void singleInsert(Connection connection, Event event) throws SQLException {
        try (
            PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement);        
                )
        {
            prepareStatementWithEvent(preparedStatement,event);
            preparedStatement.executeUpdate();   
        }
    }
    
    
    private void batchCommit(Collection<Event> events) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
        )
        {
            connection.setAutoCommit(false);
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement);
            ){
                for (Event event : events){
                    prepareStatementWithEvent(preparedStatement,event);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                logger.debug("Committing records.");
                connection.commit();
            }catch (SQLException e){
                if (!connection.isClosed()) connection.rollback();
                throw e;
            }
        }
    }
    
    private void prepareStatementWithEvent(PreparedStatement preparedStatement, Event event) throws SQLException {
        for (int i = 0; i< orderedEventParameterNames.size() ; i++){
            Object field = event.getField(orderedEventParameterNames.get(i));
            if (field instanceof Integer){
                preparedStatement.setInt(i+1, (Integer) field);
            }else if(field instanceof Double){
                preparedStatement.setDouble(i+1, (Double) field);
            }else if(field instanceof BigDecimal){
                preparedStatement.setBigDecimal(i+1, (BigDecimal) field);
            }else if(field instanceof Long){
                preparedStatement.setLong(i+1, (Long) field);
            }else if(field instanceof String){
                preparedStatement.setString(i+1, (String) field);
            }else{
                preparedStatement.setObject(i+1, field);
            }
        }
    }
    
    @Override
    public void stop() {
        stopped = true;
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(
                CONNECTION_STRING,
                SQL_STATEMENT,
                ORDERED_EVENT_PARAMETER_NAMES,
                MAX_POOL_SIZE,
                MIN_IDLE,
                DB_USERNAME,
                DB_PASSWORD
        );
    }

    @Override
    public String getId() {
        return id;
    }
}
