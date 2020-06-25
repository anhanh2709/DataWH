package util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LogUtils {
	public void insertNewLog(int config_id, String file_name, String state, Date staging_timestamp,
			Date download_timestamp, Date transform_timestamp, int staging_count, int transform_count) 
					throws ClassNotFoundException, SQLException {
		Connection con = DBConnection.ConnectControl();
		String sql = "insert into log(config_id, file_name, state, staging_timestamp, download_timestamp,"
				+ "transform_timestamp,staging_count, transform_count) values(?,?,?,?,?,?,?,?)";
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, config_id);
		ps.setString(2, file_name);
		ps.setString(3, state);
		ps.setDate(4, staging_timestamp);
		ps.setDate(5, download_timestamp);
		ps.setDate(6, transform_timestamp);
		ps.setInt(7, staging_count);
		ps.setInt(8, transform_count);
		ps.execute();
		ps.close();
	}
	public void updateNewState(String file_name, String state, Date timestamp) throws ClassNotFoundException, SQLException {
		String timestampCol = null;
		switch (state) {
		case "F":
		case "ER":
			timestampCol = "download_timestamp";
			break;
		case "TR":
			timestampCol = "staging_timestamp";
			break;
		case "SUC":
			timestampCol = "transform_timestamp";
		default:
			break;
		}
		String sql = "update log set state = ?, " +timestampCol + "= ? where file_name = ?";
		Connection con = DBConnection.ConnectControl();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, state);
		ps.setDate(2, timestamp);
		ps.setString(3, file_name);
		ps.close();
	}

}
