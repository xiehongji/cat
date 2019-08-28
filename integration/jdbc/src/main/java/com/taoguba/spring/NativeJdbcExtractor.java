package com.taoguba.spring;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface NativeJdbcExtractor {
    boolean isNativeConnectionNecessaryForNativeStatements();

    boolean isNativeConnectionNecessaryForNativePreparedStatements();

    boolean isNativeConnectionNecessaryForNativeCallableStatements();

    Connection getNativeConnection(Connection var1) throws SQLException;

    Connection getNativeConnectionFromStatement(Statement var1) throws SQLException;

    Statement getNativeStatement(Statement var1) throws SQLException;

    PreparedStatement getNativePreparedStatement(PreparedStatement var1) throws SQLException;

    CallableStatement getNativeCallableStatement(CallableStatement var1) throws SQLException;

    ResultSet getNativeResultSet(ResultSet var1) throws SQLException;
}
