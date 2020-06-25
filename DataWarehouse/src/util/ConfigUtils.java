package util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import configuration.Config;

public final class ConfigUtils {
	
	public static Config getConfig(String config_name) throws ClassNotFoundException, SQLException {
		Config config = new Config();
		String sql = "select * from config where config_name = ?";
		Connection con = DBConnection.ConnectControl();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, config_name);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			config.setConfig_id(rs.getInt("config_id"));
			config.setConfig_name(rs.getString("config_name"));
			config.setColumnList(rs.getString("config_id"));
			config.setDataTypes(rs.getString("dataTypes"));
			config.setDelimeter(rs.getString("delimeter"));
			config.setImport_dir(rs.getString("import_dir"));
			config.setSuccess_dir(rs.getString("success_dir"));
			config.setErr_dir(rs.getString("config_id"));
			config.setSrc_url(rs.getString("src_url"));
			config.setSrc_path(rs.getString("src_path"));
			config.setSrc_user(rs.getString("src_user"));
			config.setTarget_tb(rs.getString("target_tb"));
			config.setSrc_pass(rs.getString("src_pass"));
			config.setFile_Mask(rs.getString("file_mask"));
		}
		ps.close();
		return config;
	}

}
