package org.logstashplugins;

import co.elastic.logstash.api.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@LogstashPlugin(name = "logstash_jdbc_output_plugin")
public class LogstashJdbcOutputPlugin implements Output {
    
    private static final Logger logger = LogManager.getLogger(LogstashJdbcOutputPlugin.class);
    
    public static final PluginConfigSpec<String> CONNECTION_STRING =
            PluginConfigSpec.stringSetting("connectionString", "");
    
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
    
    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    
    private Long maxBatchSize;
    private String connectionString;
    private String sqlStatement;
    private List<String> orderedEventParameterNames;

    private HikariDataSource dataSource;

    public LogstashJdbcOutputPlugin(final String id, final Configuration config, final Context context) {
        
        this.id = id;
        maxBatchSize = config.get(MAX_BATCH_SIZE_CONFIG);
        connectionString = config.get(CONNECTION_STRING);
        sqlStatement = config.get(SQL_STATEMENT);
        orderedEventParameterNames = config.get(ORDERED_EVENT_PARAMETER_NAMES)
                .stream().map(o -> (String)o).collect(Collectors.toList());
        
        int maxPoolSize = config.get(MAX_POOL_SIZE).intValue();
        int minIdleConnection = config.get(MIN_IDLE).intValue();
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl( connectionString);
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
        
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement); 
                )
        {
            connection.setAutoCommit(false);
            int currentBatchSize = 0;
            for (Event event : events){
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
                preparedStatement.addBatch();
                currentBatchSize++;
                if (currentBatchSize >= maxBatchSize){
                    logger.debug("Committing {} records.",currentBatchSize);
                    preparedStatement.executeBatch();
                    currentBatchSize = 0;
                    connection.commit();
                }
            }
            if (currentBatchSize > 0 && !preparedStatement.isClosed()){
                logger.debug("LAST BATCH OP Committing {} records.",currentBatchSize);
                preparedStatement.executeBatch();
                connection.commit();
            }
            
        } catch (SQLException e) {
            logger.error("SQL Exception occurred.", e);
        }
        logger.debug(()->"Executed: "+events.size()+" in "+(System.currentTimeMillis() - startTime)+" milliseconds.");
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
                MAX_BATCH_SIZE_CONFIG,
                SQL_STATEMENT,
                ORDERED_EVENT_PARAMETER_NAMES,
                MAX_POOL_SIZE,
                MIN_IDLE
        );
    }

    @Override
    public String getId() {
        return id;
    }
}
