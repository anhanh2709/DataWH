package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnection {
	static Connection con;

	public static Connection getConnection(String database) throws ClassNotFoundException, SQLException {
		String url = "jdbc:mysql://localhost:3306/" + database;
		String user = "root";
		String password = "";
		if (con == null || con.isClosed()) {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(url, user, password);
			return con;
		} else {
			return con;
		}
	}
}
