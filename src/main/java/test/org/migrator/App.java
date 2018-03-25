package test.org.migrator;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class App {

	public static void main(String[] args) {
		App app = new App();
		app.getDataFromSqlServer(args);
	}

	public void getDataFromSqlServer(String[] args) {
		Connection sqlServerConnection = null;
		String sqlServerport = args[0];
		String sqlServerdatabaseName = args[1];
		String sqlServerUserName = args[2];
		String sqlServerPassword = args[3];
		String postgresPort = args[4];
		String postgresDatabaseName = args[5];
		String postgresUserName = args[6];
		String postgresPassword = args[7];
		try {
			sqlServerConnection = this.getSqlServerConnection(sqlServerport, sqlServerdatabaseName, sqlServerUserName,
					sqlServerPassword);
			if (sqlServerConnection != null) {
				List<String> sqlServerTables = this.getTables(sqlServerConnection, sqlServerdatabaseName);

				Statement stmt = sqlServerConnection.createStatement();
				if (!sqlServerTables.isEmpty()) {

					sqlServerTables.forEach(table -> {

						try {
							String sql = "SELECT  * FROM " + table;
							ResultSet resultSet = stmt.executeQuery(sql);
							ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
							int columnCount = resultSetMetaData.getColumnCount();
							while (resultSet.next()) {
								Object[] values = new Object[columnCount];
								for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
									values[i - 1] = resultSet.getObject(i);
								}
								this.dumpToPostgres(table, values, postgresPort, postgresDatabaseName, postgresUserName,
										postgresPassword);
							}

						} catch (SQLException | ParseException e) {

							e.printStackTrace();
						}

					});
				}

				closeConnection(sqlServerConnection);
			} else {
				System.out.println("Error: No active Connection");
			}
		} catch (Exception e) {
			closeConnection(sqlServerConnection);
			e.printStackTrace();
		}
	}

	private Connection getSqlServerConnection(String port, String databaseName, String userName, String password) {
		Connection sqlServerConnection = null;
		String connectionUrl = "jdbc:sqlserver://localhost:" + port + ";" + "databaseName=" + databaseName + ";user="
				+ userName + ";password=" + password;

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			sqlServerConnection = DriverManager.getConnection(connectionUrl);
			if (sqlServerConnection != null) {
				System.out.println("Connection Successful!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error Trace in getConnection() : " + e.getMessage());
		}
		return sqlServerConnection;
	}

	private static Connection getPostgresConnection(String portNumber, String dbName, String userName,
			String password) {
		Connection pgConnection = null;
		String connectionUrl = "jdbc:postgresql://localhost:" + portNumber + "/" + dbName;

		try {
			Class.forName("org.postgresql.Driver");
			pgConnection = DriverManager.getConnection(connectionUrl, userName, password);
			if (pgConnection != null) {
				System.out.println("Postgres Connection Successful!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error Trace in Postgres getConnection() : " + e.getMessage());
		}
		return pgConnection;
	}

	private void closeConnection(Connection con) {
		try {
			if (con != null) {
				con.close();
			}
			con = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private List<String> getTables(Connection connection, String dbName) throws SQLException {
		Statement stmt = connection.createStatement();
		String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='"
				+ dbName + "'";
		ResultSet tableSet = stmt.executeQuery(sql);
		List<String> tables = new ArrayList<String>();
		while (tableSet.next()) {
			tables.add(tableSet.getString(1));
		}
		return tables;
	}

	private void dumpToPostgres(String table, Object[] rowData, String port, String dbName, String user,
			String password) throws SQLException, ParseException {
		Connection pgConnection = App.getPostgresConnection(port, dbName, user, password);

		if (pgConnection != null) {
			String sql = "insert into " + table + " values (";
			for (int i = 1; i <= rowData.length; i++) {
				sql += "?,";
			}
			sql = sql.replaceAll(",$", "");
			sql += ")";
			PreparedStatement pstmt = pgConnection.prepareStatement(sql);
			for (int i = 1; i <= rowData.length; i++) {
				if (rowData[i - 1] instanceof Timestamp) {
					String resultSetTimeStamp = String.valueOf(rowData[i - 1]);
					String testDate = resultSetTimeStamp.substring(0, resultSetTimeStamp.lastIndexOf("."));
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
					Date parsedTimeStamp = new Date(dateFormat.parse(testDate).getTime());
					pstmt.setObject(i, parsedTimeStamp);
				}
				pstmt.setObject(i, rowData[i - 1]);
			}

			pstmt.executeUpdate();

			pstmt.close();
		}
		closeConnection(pgConnection);
	}
}
