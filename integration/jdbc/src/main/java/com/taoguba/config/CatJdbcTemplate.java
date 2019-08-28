
package com.taoguba.config;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.taoguba.spring.NativeJdbcExtractor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;


import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author : Ares.yi
 * @createTime : 2019-1-19
 * @version : 1.0
 * @description :
 *
 */
public class CatJdbcTemplate extends JdbcTemplate {

    /** Custom NativeJdbcExtractor */
    private NativeJdbcExtractor nativeJdbcExtractor;


    public CatJdbcTemplate() {
        super();
    }


    public CatJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }


    public CatJdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource,lazyInit);
    }

    public void setNativeJdbcExtractor(NativeJdbcExtractor extractor) {
        this.nativeJdbcExtractor = extractor;
    }


    public NativeJdbcExtractor getNativeJdbcExtractor() {
        return this.nativeJdbcExtractor;
    }


    @Override
    public <T> T execute(ConnectionCallback<T> action) throws DataAccessException {

        Assert.notNull(action, "Callback object must not be null");

        String sql = getSql(action);
        Transaction t = Cat.newTransaction("SQL", getSubSql(sql));

        Connection con = DataSourceUtils.getConnection(getDataSource());
        try {

            Cat.logEvent("SQL.Database", this.getSQLDatabase());
            Cat.logEvent("SQL.Method", getSqlType(sql), Message.SUCCESS, sql);

            Connection conToUse = con;
            if (this.nativeJdbcExtractor != null) {
                // Extract native JDBC Connection, castable to OracleConnection or the like.
                conToUse = this.nativeJdbcExtractor.getNativeConnection(con);
            }
            else {
                // Create close-suppressing Connection proxy, also preparing returned Statements.
                conToUse = createConnectionProxy(con);
            }

            T res =  action.doInConnection(conToUse);

            t.setStatus(Transaction.SUCCESS);
            return res;
        }
        catch (SQLException ex) {
            t.setStatus(ex);
            DataSourceUtils.releaseConnection(con, getDataSource());
            con = null;
            throw getExceptionTranslator().translate("ConnectionCallback", sql, ex);
        }
        finally {
            DataSourceUtils.releaseConnection(con, getDataSource());

            t.complete();
        }
    }


    //-------------------------------------------------------------------------
    // Methods dealing with static SQL (java.sql.Statement)
    //-------------------------------------------------------------------------

    @Override
    public <T> T execute(StatementCallback<T> action) throws DataAccessException {
        Assert.notNull(action, "Callback object must not be null");

        String sql = getSql(action);
        Transaction t = Cat.newTransaction("SQL", getSubSql(sql));

        Connection con = DataSourceUtils.getConnection(getDataSource());
        Statement stmt = null;
        try {

            Cat.logEvent("SQL.Database", this.getSQLDatabase());
            Cat.logEvent("SQL.Method", getSqlType(sql), Message.SUCCESS, sql);

            Connection conToUse = con;
            if (this.nativeJdbcExtractor != null &&
                    this.nativeJdbcExtractor.isNativeConnectionNecessaryForNativeStatements()) {
                conToUse = this.nativeJdbcExtractor.getNativeConnection(con);
            }
            stmt = conToUse.createStatement();
            applyStatementSettings(stmt);
            Statement stmtToUse = stmt;
            if (this.nativeJdbcExtractor != null) {
                stmtToUse = this.nativeJdbcExtractor.getNativeStatement(stmt);
            }
            T result = action.doInStatement(stmtToUse);
            handleWarnings(stmt);

            t.setStatus(Transaction.SUCCESS);
            return result;
        }
        catch (SQLException ex) {
            t.setStatus(ex);
            // Release Connection early, to avoid potential connection pool deadlock
            // in the case when the exception translator hasn't been initialized yet.
            JdbcUtils.closeStatement(stmt);
            stmt = null;
            DataSourceUtils.releaseConnection(con, getDataSource());
            con = null;
            throw getExceptionTranslator().translate("StatementCallback",sql, ex);
        }
        finally {
            JdbcUtils.closeStatement(stmt);
            DataSourceUtils.releaseConnection(con, getDataSource());
            t.complete();
        }
    }


    //-------------------------------------------------------------------------
    // Methods dealing with prepared statements
    //-------------------------------------------------------------------------

    @Override
    public <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action)
            throws DataAccessException {

        Assert.notNull(psc, "PreparedStatementCreator must not be null");
        Assert.notNull(action, "Callback object must not be null");
        if (logger.isDebugEnabled()) {
            String sql = getSql(psc);
            logger.debug("Executing prepared SQL statement" + (sql != null ? " [" + sql + "]" : ""));
        }

        String sql = getSql(psc);
        System.out.println(action.toString());
        Transaction t = Cat.newTransaction("SQL", getSubSql(sql));

        Connection con = DataSourceUtils.getConnection(getDataSource());
        PreparedStatement ps = null;
        try {

            Cat.logEvent("SQL.Database", this.getSQLDatabase());
            Cat.logEvent("SQL.Method", getSqlType(sql), Message.SUCCESS, sql);

            Connection conToUse = con;
            if (this.nativeJdbcExtractor != null &&
                    this.nativeJdbcExtractor.isNativeConnectionNecessaryForNativePreparedStatements()) {
                conToUse = this.nativeJdbcExtractor.getNativeConnection(con);
            }
            ps = psc.createPreparedStatement(conToUse);
            applyStatementSettings(ps);
            PreparedStatement psToUse = ps;
            if (this.nativeJdbcExtractor != null) {
                psToUse = this.nativeJdbcExtractor.getNativePreparedStatement(ps);
            }
            T result = action.doInPreparedStatement(psToUse);
            handleWarnings(ps);

            t.setStatus(Transaction.SUCCESS);
            return result;
        }
        catch (SQLException ex) {
            t.setStatus(ex);
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }

            psc = null;
            JdbcUtils.closeStatement(ps);
            ps = null;
            DataSourceUtils.releaseConnection(con, getDataSource());
            con = null;
            throw getExceptionTranslator().translate("PreparedStatementCallback", sql, ex);
        }
        finally {
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }
            JdbcUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(con, getDataSource());
            t.complete();
        }
    }



    //-------------------------------------------------------------------------
    // Methods dealing with callable statements
    //-------------------------------------------------------------------------

    @Override
    public <T> T execute(CallableStatementCreator csc, CallableStatementCallback<T> action)
            throws DataAccessException {

        Assert.notNull(csc, "CallableStatementCreator must not be null");
        Assert.notNull(action, "Callback object must not be null");
        if (logger.isDebugEnabled()) {
            String sql = getSql(csc);
            logger.debug("Calling stored procedure" + (sql != null ? " [" + sql  + "]" : ""));
        }

        String sql = getSql(csc);
        Transaction t = Cat.newTransaction("SQL", getSubSql(sql));

        Connection con = DataSourceUtils.getConnection(getDataSource());
        CallableStatement cs = null;
        try {

            Cat.logEvent("SQL.Database", this.getSQLDatabase());
            Cat.logEvent("SQL.Method", getSqlType(sql), Message.SUCCESS, sql);

            Connection conToUse = con;
            if (this.nativeJdbcExtractor != null) {
                conToUse = this.nativeJdbcExtractor.getNativeConnection(con);
            }
            cs = csc.createCallableStatement(conToUse);
            applyStatementSettings(cs);
            CallableStatement csToUse = cs;
            if (this.nativeJdbcExtractor != null) {
                csToUse = this.nativeJdbcExtractor.getNativeCallableStatement(cs);
            }
            T result = action.doInCallableStatement(csToUse);
            handleWarnings(cs);

            t.setStatus(Transaction.SUCCESS);
            return result;
        }
        catch (SQLException ex) {
            t.setStatus(ex);
            // Release Connection early, to avoid potential connection pool deadlock
            // in the case when the exception translator hasn't been initialized yet.
            if (csc instanceof ParameterDisposer) {
                ((ParameterDisposer) csc).cleanupParameters();
            }
            csc = null;
            JdbcUtils.closeStatement(cs);
            cs = null;
            DataSourceUtils.releaseConnection(con, getDataSource());
            con = null;
            throw getExceptionTranslator().translate("CallableStatementCallback", sql, ex);
        }
        finally {
            if (csc instanceof ParameterDisposer) {
                ((ParameterDisposer) csc).cleanupParameters();
            }
            JdbcUtils.closeStatement(cs);
            DataSourceUtils.releaseConnection(con, getDataSource());
            t.complete();
        }
    }

    private static String getSql(Object sqlProvider) {
        if (sqlProvider instanceof SqlProvider) {
            return ((SqlProvider) sqlProvider).getSql();
        }else {
            return "";
        }
    }

    private static String getSubSql(String sql) {
        try {
            return sql.substring(0,Math.min(25,sql.length()));
        } catch (Exception e) {
            return "springJdbc";
        }
    }

    private static String getSqlType(String sql) {
        try {
            if (sql==null || sql.equals("")){
                return "UN";
            }
            return  sql.substring(0,sql.indexOf(" "));
        } catch (Exception e) {
            e.printStackTrace();
            Cat.logError(e);
        }
        return "err";
    }

    private static final Map<String, String> sqlURLCache = new ConcurrentHashMap<String, String>(256);
    private static final String EMPTY_CONNECTION = "jdbc:mysql://unknown:3306/%s?useUnicode=true";

    // druid 数据源的类名称
    private static final String DruidDataSourceClassName = "com.alibaba.druid.pool.DruidDataSource";
    // dbcp 数据源的类名称
    private static final String DBCPBasicDataSourceClassName = "org.apache.commons.dbcp.BasicDataSource";
    // dbcp2 数据源的类名称
    private static final String DBCP2BasicDataSourceClassName = "org.apache.commons.dbcp2.BasicDataSource";
    // c3p0 数据源的类名称
    private static final String C3P0ComboPooledDataSourceClassName = "com.mchange.v2.c3p0.ComboPooledDataSource";
    // HikariCP 数据源的类名称
    private static final String HikariCPDataSourceClassName = "com.zaxxer.hikari.HikariDataSource";
    // BoneCP 数据源的类名称
    private static final String BoneCPDataSourceClassName = "com.jolbox.bonecp.BoneCPDataSource";
    // Tomcat
    private static final String TomcatJdbcDataSourceClassName = "org.apache.tomcat.jdbc.pool.DataSource";

    private String getSQLDatabase() {
        String dbName = null;
        if (dbName == null) {
            dbName = "DEFAULT";
        }
        String url = sqlURLCache.get(dbName);
        if (url != null) {
            return url;
        }

        url = this.getJdbcUrl();//目前监控只支持mysql ,其余数据库需要各自修改监控服务端
        if (url == null) {
            url = String.format(EMPTY_CONNECTION, dbName);
        }
        sqlURLCache.put(dbName, url);
        return url;
    }

    private String getJdbcUrl() {
        DataSource dataSource = this.getDataSource();
        if (dataSource == null) {
            return "-";
        }
        if(DruidDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, DruidDataSourceClassName, "getUrl");
        }else if(DBCPBasicDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, DBCPBasicDataSourceClassName, "getUrl");
        }else if(DBCP2BasicDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, DBCP2BasicDataSourceClassName, "getUrl");
        }else if(C3P0ComboPooledDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, C3P0ComboPooledDataSourceClassName, "getJdbcUrl");
        }else if(HikariCPDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, HikariCPDataSourceClassName, "getJdbcUrl");
        }else if(BoneCPDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, BoneCPDataSourceClassName, "getJdbcUrl");
        }else if(TomcatJdbcDataSourceClassName.equals(DruidDataSourceClassName)){
            return getDataSourceJdbcURL(dataSource, TomcatJdbcDataSourceClassName, "getUrl");
        }else{
            return "--";
        }
    }


    private String getDataSourceJdbcURL(DataSource dataSource, String runtimeDataSourceClassName, String sqlURLMethodName) {
        Class<?> dataSourceClass = null;
        try {
            dataSourceClass = Class.forName(runtimeDataSourceClassName);
            Method sqlURLMethod = ReflectionUtils.findMethod(dataSourceClass, sqlURLMethodName);
            return (String) ReflectionUtils.invokeMethod(sqlURLMethod, dataSource);
        } catch (ClassNotFoundException e) {
            Cat.logError(e);
        }

        return "-";
    }


}
