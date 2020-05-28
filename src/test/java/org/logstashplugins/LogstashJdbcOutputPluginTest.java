package org.logstashplugins;

import co.elastic.logstash.api.*;
import com.thedeanda.lorem.Lorem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.logstash.plugins.ConfigurationImpl;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

import static org.logstashplugins.LogstashJdbcOutputPlugin.*;

public class LogstashJdbcOutputPluginTest {
    
    private Logger logger = LogManager.getLogger(this.getClass());
    
    public LogstashJdbcOutputPlugin configure(String dbName){
        logger.info("Starting to configure logstash.");
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(CONNECTION_STRING.name(),"jdbc:hsqldb:mem:"+dbName);
        configValues.put(DB_USERNAME.name(),"evam");
        configValues.put(DB_PASSWORD.name(),"evam");
        configValues.put(SQL_STATEMENT.name(),"insert into test_table (ID,NAME,SURNAME,AGE) VALUES(?,?,?,?)");
        configValues.put(ORDERED_EVENT_PARAMETER_NAMES.name(), 
                Arrays.asList(
                        "ID",
                        "NAME",
                        "SURNAME",
                        "AGE"
                )
        );
        Configuration config = new ConfigurationImpl(configValues);
        Context context = new Context() {
            @Override
            public DeadLetterQueueWriter getDlqWriter() {
                return new MockedDLQWriter(logger);
            }

            @Override
            public NamespacedMetric getMetric(Plugin plugin) {
                return null;
            }

            @Override
            public Logger getLogger(Plugin plugin) {
                return logger;
            }

            @Override
            public EventFactory getEventFactory() {
                return null;
            }
        };
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = new LogstashJdbcOutputPlugin("id",config,context);
        return logstashJdbcOutputPlugin;
    }
    
    private void create_table(LogstashJdbcOutputPlugin logstashJdbcOutputPlugin) throws SQLException {
        DataSource dataSource = logstashJdbcOutputPlugin.getDataSource();
        try (
                Connection connection = dataSource.getConnection();
                Statement stmt = connection.createStatement()
        )
        {

            int result = stmt.executeUpdate(
                    "CREATE TABLE test_table ("+
                            "id INT NOT NULL, " +
                            "name VARCHAR(50) NOT NULL,"+
                            "surname VARCHAR(20), " +
                            "age INT,"+
                            "PRIMARY KEY (id));");
            System.out.println(result);
        }
    }
    
    private int count_records(DataSource dataSource) throws SQLException {
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement stmt = connection.prepareStatement("select count(*) as c from test_table")
        )
        {
            
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()){
                return resultSet.getInt("c");
            }else{
                return 0;
            }
        }
    }
    
    @Test
    public void instantiate(){
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("instantiate");
    }
    
    @Test
    public void create_table() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("create_table");
        create_table(logstashJdbcOutputPlugin);
    }

    @Test
    public void happy_test() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("happy_test");
        create_table(logstashJdbcOutputPlugin);

        Random random = new Random();
        final int maxEvent = 100000;
        final int batchSize = 250;
        List<Event> events = new ArrayList<>(batchSize);
        for (int i = 0; i <maxEvent;i++){
            
            Event event = new org.logstash.Event();
            event.setField("ID",i);
            event.setField("NAME",Lorem.getFirstName());
            event.setField("SURNAME",Lorem.getLastName());
            event.setField("AGE",random.nextInt(65));
            events.add(event);
            
            if (events.size() >= batchSize){
                logstashJdbcOutputPlugin.output(events);
                events.clear();
            }
        }

        if (!events.isEmpty()){
            logstashJdbcOutputPlugin.output(events);
            events.clear();
        }
        
        Assertions.assertEquals(maxEvent,count_records(logstashJdbcOutputPlugin.getDataSource()));
    }
    
    @Test
    public void testDataIntegrityViolationCase() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("test_data_integrity_violation");
        create_table(logstashJdbcOutputPlugin);
        
        Exception e = Assertions.assertThrows(IllegalStateException.class,() -> {
            Random random = new Random();
            final int maxEvent = 100000;
            final int batchSize = 250;
            final int atThisInsertDuplicatedPrimaryKey = 1020;
            List<Event> events = new ArrayList<>(batchSize);
            for (int i = 0; i <maxEvent;i++){

                Event event = new org.logstash.Event();
                event.setField("ID",i == atThisInsertDuplicatedPrimaryKey ? 1 : i);
                event.setField("NAME",Lorem.getFirstName());
                event.setField("SURNAME",Lorem.getLastName());
                event.setField("AGE",random.nextInt(65));
                events.add(event);

                if (events.size() >= batchSize){
                    logstashJdbcOutputPlugin.output(events);
                    events.clear();
                }
            }

            if (!events.isEmpty()){
                logstashJdbcOutputPlugin.output(events);
                events.clear();
            }
        });
        
        Assertions.assertEquals(1000,count_records(logstashJdbcOutputPlugin.getDataSource()));
        
    }

    
    @Test
    public void testConnectionCloseViolationWhileBatchUpdateCase() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("test_connection_close");
        create_table(logstashJdbcOutputPlugin);
        final int maxEvent = 100000;
        final int batchSize = 250;
        final int atThisCloseConnections = 1001;
        final int closeForSeconds = 10;
        
        DataSource actualDataSource = logstashJdbcOutputPlugin.getDataSource();
        DataSource mockedDataSource = new MockDataSource(new SQLException("","",-80));
        Random random = new Random();
       
        List<Event> events = new ArrayList<>(batchSize);
        for (int i = 0; i <maxEvent;i++){
            if (i == atThisCloseConnections){
                logstashJdbcOutputPlugin.setDataSource(mockedDataSource);
                System.out.println("Changed datasource with mocked one that throws DataAccessResourceFailureException");
                new Timer().schedule(
                        new TimerTask() {
                            @Override
                            public void run() {
                                System.out.println("Changed datasource with proper one");
                                logstashJdbcOutputPlugin.setDataSource(actualDataSource);
                            }
                        }
                , closeForSeconds * 1000);
            }
            Event event = new org.logstash.Event();
            event.setField("ID",i);
            event.setField("NAME",Lorem.getFirstName());
            event.setField("SURNAME",Lorem.getLastName());
            event.setField("AGE",random.nextInt(65));
            events.add(event);

            if (events.size() >= batchSize){
                logstashJdbcOutputPlugin.output(events);
                events.clear();
            }
        }

        if (!events.isEmpty()){
            logstashJdbcOutputPlugin.output(events);
            events.clear();
        }

        Assertions.assertEquals(maxEvent,count_records(logstashJdbcOutputPlugin.getDataSource()));
    }

    @Test
    public void testConnectionCloseViolationWhileBatchUpdateCloseSignal() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("test_connection_close");
        create_table(logstashJdbcOutputPlugin);
        final int maxEvent = 100000;
        final int batchSize = 250;
        final int atThisCloseConnections = 1001;
        final int closeForSeconds = 10;

        DataSource actualDataSource = logstashJdbcOutputPlugin.getDataSource();
        DataSource mockedDataSource = new MockDataSource(new SQLException("","",-80));
        Random random = new Random();

        List<Event> events = new ArrayList<>(batchSize);
        for (int i = 0; i <maxEvent;i++){
            if (i == atThisCloseConnections){
                logstashJdbcOutputPlugin.setDataSource(mockedDataSource);
                System.out.println("Changed datasource with mocked one that throws DataAccessResourceFailureException");
                new Timer().schedule(
                        new TimerTask() {
                            @Override
                            public void run() {
                                System.out.println("Changed datasource with proper one");
                                logstashJdbcOutputPlugin.setDataSource(actualDataSource);
                            }
                        }
                        , closeForSeconds * 1000);
                
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        System.out.println("Sending stop");
                        logstashJdbcOutputPlugin.stop();
                    }
                }, 5*1000);
            }
            Event event = new org.logstash.Event();
            event.setField("ID",i);
            event.setField("NAME",Lorem.getFirstName());
            event.setField("SURNAME",Lorem.getLastName());
            event.setField("AGE",random.nextInt(65));
            events.add(event);

            if (events.size() >= batchSize){
                logstashJdbcOutputPlugin.output(events);
                events.clear();
            }
        }

        if (!events.isEmpty()){
            logstashJdbcOutputPlugin.output(events);
            events.clear();
        }

        Assertions.assertEquals(maxEvent,count_records(logstashJdbcOutputPlugin.getDataSource()));
    }
    
    @Test
    public void corrupted_event() throws SQLException {
        LogstashJdbcOutputPlugin logstashJdbcOutputPlugin = configure("corrupted_event");
        create_table(logstashJdbcOutputPlugin);
        final int maxEvent = 100000;
        final int batchSize = 250;
        final int atThisAddCorruptedEvent = 1001;
        
        Random random = new Random();

        List<Event> events = new ArrayList<>(batchSize);
        for (int i = 0; i <maxEvent;i++){
            Event event = new org.logstash.Event();
            event.setField("ID",i);
            event.setField("NAME",Lorem.getFirstName());
            event.setField("SURNAME",Lorem.getLastName());
            event.setField("AGE",random.nextInt(65));
            if (i == atThisAddCorruptedEvent){
                event.setField("AGE","asdsadsa");    
            }
          
            events.add(event);

            if (events.size() >= batchSize){
                logstashJdbcOutputPlugin.output(events);
                events.clear();
            }
        }

        if (!events.isEmpty()){
            logstashJdbcOutputPlugin.output(events);
            events.clear();
        }

        Assertions.assertEquals(maxEvent - 1,count_records(logstashJdbcOutputPlugin.getDataSource()));
    }
    
    
}
