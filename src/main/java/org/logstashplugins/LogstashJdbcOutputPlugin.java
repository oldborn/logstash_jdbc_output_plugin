package org.logstashplugins;

import co.elastic.logstash.api.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.DLQEntry;
import org.logstash.common.DeadLetterQueueFactory;
import org.logstash.common.io.DeadLetterQueueWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    
    public static final PluginConfigSpec<Long> MAX_BATCH_SIZE_CONFIG =
            PluginConfigSpec.numSetting("maxBatchSize", 10000);
    
    public static final PluginConfigSpec<String> SQL_STATEMENT =
            PluginConfigSpec.stringSetting("sqlStatement", "");
    
    public static final PluginConfigSpec<List<Object>> ORDERED_EVENT_PARAMETER_NAMES =
            PluginConfigSpec.arraySetting("orderedEventParameterNames");
    
    public static final PluginConfigSpec<Long> MAX_POOL_SIZE =
            PluginConfigSpec.numSetting("maxPoolSize", 10);

    public static final PluginConfigSpec<Long> MIN_IDLE =
            PluginConfigSpec.numSetting("minIdle", 10);

    public static final PluginConfigSpec<String> DEAD_LETTER_QUEUE_PATH =
            PluginConfigSpec.stringSetting("deadLetterQueuePath");

    public static final PluginConfigSpec<Long> DEAD_LETTER_QUEUE_MAX_SIZE =
            PluginConfigSpec.numSetting("deadLetterQueueMaxSize",1000000);
    
    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    
    private Long maxBatchSize;
    
    private String sqlStatement;
    private List<String> orderedEventParameterNames;

    private Optional<DeadLetterQueueWriter> deadLetterQueueWriterOptional;
    
    private HikariDataSource dataSource;

    public LogstashJdbcOutputPlugin(final String id, final Configuration config, final Context context) {
        logger = context.getLogger(this);
        String deadLetterQueuePath = config.get(DEAD_LETTER_QUEUE_PATH);
        if (deadLetterQueuePath != null){
            Long deadLetterQueueMaxSize = config.get(DEAD_LETTER_QUEUE_MAX_SIZE);
            logger.info("Configuring DLQ with deadLetterQueuePath {}, deadLetterQueueMaxSize {}",deadLetterQueuePath, deadLetterQueueMaxSize);
            deadLetterQueueWriterOptional = Optional.ofNullable(DeadLetterQueueFactory.getWriter(id,deadLetterQueuePath,deadLetterQueueMaxSize));
        }else{
            deadLetterQueueWriterOptional = Optional.empty();
        }
        this.id = id;
        maxBatchSize = config.get(MAX_BATCH_SIZE_CONFIG);
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
        
        hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMinimumIdle(minIdleConnection);
        logger.info("Initializing HikariPool");
        dataSource = new HikariDataSource( hikariConfig );
        logger.info("Initialized HikariPool");
    }

    @Override
    public void output(final Collection<Event> events) {
        long startTime = System.currentTimeMillis();
        logger.debug(()->"Number of events: "+events.size()+" ");
        List<Event> batchedEvents = new ArrayList<>();
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement); 
                )
        {
            connection.setAutoCommit(false);
            for (Event event : events){
                try {
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
                    batchedEvents.add(event);
                    preparedStatement.addBatch();
                    
                    if (batchedEvents.size() >= maxBatchSize){
                        logger.debug("Committing {} records.",batchedEvents.size());
                        preparedStatement.executeBatch();
                        connection.commit();
                        batchedEvents.clear();
                    }
                } catch (SQLException e) {
                    logger.error("SQL Exception occured. Adding to DLQ.",e);
                    addToDLQ(batchedEvents,e.getMessage());
                }

            }
            if (batchedEvents.size() > 0 && !preparedStatement.isClosed()){
                logger.debug("LAST BATCH OP Committing {} records.",batchedEvents.size());
                preparedStatement.executeBatch();
                connection.commit();
                batchedEvents.clear();
            }
            
        } catch (SQLException e) {
            logger.error("SQL Exception occured. Adding to DLQ.",e);
            addToDLQ(batchedEvents,e.getMessage());
        }
        logger.debug(()->"Executed: "+events.size()+" in "+(System.currentTimeMillis() - startTime)+" milliseconds.");
    }

    private void addToDLQ(List<Event> events,String reason){
        deadLetterQueueWriterOptional.orElseThrow(() -> new RuntimeException("DLQ is not configured"));
        DeadLetterQueueWriter deadLetterQueueWriter = deadLetterQueueWriterOptional.get();
        for (Event batchedEvent : events){
            org.logstash.Event logstashEvent = new org.logstash.Event();
            logstashEvent.append(batchedEvent);
            try {
                deadLetterQueueWriter.writeEntry(
                        logstashEvent,
                        "logstash_jdbc_output_plugin",
                        "logstash_jdbc_output_plugin",
                        reason
                );
            } catch (IOException ex) {
                throw new RuntimeException("DLQ writer is not available.",ex);
            }
        }
        events.clear();
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
                DEAD_LETTER_QUEUE_PATH,
                DEAD_LETTER_QUEUE_MAX_SIZE,
                CONNECTION_STRING, 
                MAX_BATCH_SIZE_CONFIG,
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
