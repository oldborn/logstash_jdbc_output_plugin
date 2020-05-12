package org.logstashplugins;

import org.apache.logging.log4j.Logger;
import org.springframework.dao.*;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.lang.Nullable;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Created by: Safak T. at 5/7/2020
 * While listening: SCREWS - DREAMERS @Link https://open.spotify.com/track/1GBCRzguynFjvk7r7cyunV
 */
public class SQLExceptionStrategyDecider {
    private SQLErrorCodeSQLExceptionTranslator sqlExceptionTranslator;
    private Optional<Logger> loggerOptional;
    
    public SQLExceptionStrategyDecider(DataSource dataSource) {
        sqlExceptionTranslator = new SQLErrorCodeSQLExceptionTranslator(dataSource);
    }
    
    public SQLExceptionStrategyDecider(DataSource dataSource, @Nullable Logger logger) {
        sqlExceptionTranslator = new SQLErrorCodeSQLExceptionTranslator(dataSource);
        loggerOptional = Optional.ofNullable(logger);
    }
    
    public SQLExceptionStrategyDecider(String dbName, @Nullable Logger logger) {
        sqlExceptionTranslator = new SQLErrorCodeSQLExceptionTranslator(dbName);
        loggerOptional = Optional.ofNullable(logger);
    }

    public SQLExceptionResolveStrategy decide(SQLException exception){
        DataAccessException dataAccessException = sqlExceptionTranslator.translate("",null,exception);
        if (dataAccessException instanceof BadSqlGrammarException){
            return SQLExceptionResolveStrategy.SHUTDOWN;
        }else if (dataAccessException instanceof InvalidResultSetAccessException){
            return SQLExceptionResolveStrategy.SHUTDOWN;
        }else if (dataAccessException instanceof DuplicateKeyException){
            return SQLExceptionResolveStrategy.SHUTDOWN;
        }else if (dataAccessException instanceof DataIntegrityViolationException){
            return SQLExceptionResolveStrategy.DISCARD;
        }else if (dataAccessException instanceof PermissionDeniedDataAccessException){
            return SQLExceptionResolveStrategy.RETRY;
        }else if (dataAccessException instanceof DataAccessResourceFailureException){
            return SQLExceptionResolveStrategy.RETRY;
        }else if (dataAccessException instanceof TransientDataAccessResourceException){
            return SQLExceptionResolveStrategy.RETRY;
        }else if (dataAccessException instanceof CannotAcquireLockException){
            return SQLExceptionResolveStrategy.RETRY;
        }else if (dataAccessException instanceof DeadlockLoserDataAccessException){
            return SQLExceptionResolveStrategy.RETRY;
        }else if (dataAccessException instanceof CannotSerializeTransactionException){
            return SQLExceptionResolveStrategy.RETRY;
        }
        
        loggerOptional.ifPresent(logger -> logger.error("UNKNOWN SQLException: {}.",exception.getClass().getName()));
        return SQLExceptionResolveStrategy.DISCARD;
    }
}
