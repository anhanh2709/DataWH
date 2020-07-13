package util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import configuration.Config;
import configuration.Log;

public class LogUtils {
	static Connection con;
	static {
		try {
			con = DBConnection.ConnectControl();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void insertNewLog(int config_id, String file_name, String state, Timestamp staging_timestamp,
			Timestamp download_timestamp, Timestamp transform_timestamp, int staging_count, int transform_count) 
					throws ClassNotFoundException, SQLException {
		
		String sql = "insert into log(config_id, file_name, state, staging_timestamp, download_timestamp,"
				+ "transform_timestamp,staging_count, transform_count) values(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, config_id);
		ps.setString(2, file_name);
		ps.setString(3, state);
		ps.setTimestamp(4, staging_timestamp);
		ps.setTimestamp(5, download_timestamp);
		ps.setTimestamp(6, transform_timestamp);
		ps.setInt(7, staging_count);
		ps.setInt(8, transform_count);
		ps.execute();
		ps.close();
	}
	public static void updateNewState(int config_id, String state, Timestamp timestamp, int count) throws ClassNotFoundException, SQLException {
		String timestampCol = null;
		String countCol = null;
		switch (state) {
		case "F":
		case "ER":
			timestampCol = "download_timestamp";
			countCol = "staging_count";
			break;
		case "EXS":
		case "EXF":
			timestampCol = "staging_timestamp";
			countCol = "staging_count";
			break;
		case "SUC":
			timestampCol = "transform_timestamp";
			countCol = "transform_count";
			break;
		default:
			break;
		}
		String sql = "update log set state = ?, " +timestampCol + "= ?," +countCol+ "= ? where config_id = ?";
		System.out.println(sql);
		Connection con = DBConnection.ConnectControl();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, state);
		ps.setTimestamp(2, timestamp);
		ps.setInt(3, count);
		ps.setInt(4, config_id);
		ps.execute();
		ps.close();
	}
	public static List<Integer> getConfigIDByState(String state) {
		List<Integer> listConfig = new ArrayList<Integer>();
		try {
			
			String sql = "Select distinct config_id from  log where state = ?";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, state);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				listConfig.add(rs.getInt("config_id"));
			}
			ps.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return listConfig;
	}
	public static List<Log> getConfigByState(String state) {
		List<Log> listConfig = new ArrayList<Log>();
		try {
			String sql = "Select * from log where state = ?";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, state);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				listConfig.add(new Log(rs.getInt("config_id"),
						rs.getString("file_name"),
						rs.getString("state"),
						rs.getInt("staging_count"),
						rs.getInt("transform_count")));
			}
			ps.close();
			
		} catch (SQLException e) {
	
			e.printStackTrace();
		}
		return listConfig;
	}


}
