package util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

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
			config.setErr_dir(rs.getString("err_dir"));
			config.setSrc_url(rs.getString("src_url"));
			config.setSrc_path(rs.getString("src_path"));
			config.setSrc_user(rs.getString("src_user"));
			config.setTarget_tb(rs.getString("target_tb"));
			config.setSrc_pass(rs.getString("src_pass"));
			config.setFile_Mask(rs.getString("file_mask"));
			config.setFile_type(rs.getString("file_type"));
			config.setColumnList(rs.getString("columnList"));
		}
		ps.close();
		return config;
	}
	public static Config getConfigByID(int config_id) throws ClassNotFoundException, SQLException {
		Config config = new Config();
		String sql = "select * from config where config_id = ?";
		Connection con = DBConnection.ConnectControl();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, config_id);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			config.setConfig_id(rs.getInt("config_id"));
			config.setConfig_name(rs.getString("config_name"));
			config.setColumnList(rs.getString("config_id"));
			config.setDataTypes(rs.getString("dataTypes"));
			config.setDelimeter(rs.getString("delimeter"));
			config.setImport_dir(rs.getString("import_dir"));
			config.setSuccess_dir(rs.getString("success_dir"));
			config.setErr_dir(rs.getString("err_dir"));
			config.setSrc_url(rs.getString("src_url"));
			config.setSrc_path(rs.getString("src_path"));
			config.setSrc_user(rs.getString("src_user"));
			config.setTarget_tb(rs.getString("target_tb"));
			config.setSrc_pass(rs.getString("src_pass"));
			config.setFile_Mask(rs.getString("file_mask"));
			config.setFile_type(rs.getString("file_type"));
			config.setColumnList(rs.getString("columnList"));
		}
		ps.close();
		return config;
	}
	public static Config getConfigAutoRun() throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String sql = "Call control.getNextRunConFig(?)";
		
		CallableStatement clstm = DBConnection.ConnectControl().prepareCall(sql);
		int config_id = 0;
		clstm.registerOutParameter(1, Types.INTEGER);
		clstm.execute();
		config_id = clstm.getInt(1);
		System.out.println(config_id);
		Config config  = ConfigUtils.getConfigByID(config_id);
		return config;
	}

}
