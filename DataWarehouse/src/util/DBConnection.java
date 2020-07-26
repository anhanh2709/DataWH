package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnection {
	static Connection con = null;

	public static Connection getConnection(String url, String user, String password) throws ClassNotFoundException, SQLException {
		if (con == null || con.isClosed()) {
			con = DriverManager.getConnection(url, user, password);
			return con;
		}
		return con;
	}
	public static Connection ConnectControl() throws ClassNotFoundException, SQLException {
		 String url = "jdbc:mysql://localhost:3306/control";
		 String user = "root";
		 String password = "StrongPass123";
		return getConnection(url, user, password);
	}
	public static Connection ConnectStaging() throws ClassNotFoundException, SQLException {
		return getConnectionFromControl("Staging");
	}
	public static Connection ConnectWareHouse() throws ClassNotFoundException, SQLException {
		return getConnectionFromControl("WareHouse");
	}
	public static Connection getConnectionFromControl(String db) throws ClassNotFoundException, SQLException {
		Connection con = ConnectControl();
		String sql = "select con_url, user, password from connection where db_name = ?";
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, db);
		ResultSet rs = ps.executeQuery();
		String url = null, user = null, password = null;
		while(rs.next()) {
			url = rs.getString("con_url");
			user = rs.getString("user");
			password = rs.getString("password");
		}
		ps.close();
		return getConnection(url, user, password);
		
	}
}
