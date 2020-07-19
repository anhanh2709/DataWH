package dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import configuration.Config;
import util.DBConnection;

public class ControlDatabase {
	private String config_db_name;
	private String target_db_name;
	private String table_name;
	private PreparedStatement pst = null;
	private ResultSet rs = null;
	private String sql;

	public ControlDatabase(String db_name, String table_name, String target_db_name) {
		this.config_db_name = db_name;
		this.table_name = table_name;
		this.target_db_name = target_db_name;
	}

	public ControlDatabase() {
	}

	public String getConfig_db_name() {
		return config_db_name;
	}

	public void setConfig_db_name(String config_db_name) {
		this.config_db_name = config_db_name;
	}

	public String getTarget_db_name() {
		return target_db_name;
	}

	public void setTarget_db_name(String target_db_name) {
		this.target_db_name = target_db_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public boolean tableExist(String table_name) throws ClassNotFoundException {
		try {
			DatabaseMetaData dbm = DBConnection.ConnectStaging().getMetaData();
			ResultSet tables = dbm.getTables(null, null, table_name, null);
			try {
				if (tables.next()) {
					System.out.println(true);
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
				System.out.println(false);
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}
	
	//lay listconfig
	public static List<Config> loadAllConfig(String config_name) throws ClassNotFoundException, SQLException {
		List<Config> listConfig = new ArrayList<Config>();
		String sql = "select * from config where config_name = ?";
		Connection con = DBConnection.ConnectControl();
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setString(1, config_name);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			Config config = new Config();
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
			listConfig.add(config);
		}
		ps.close();
		return listConfig;
	}


	public boolean insertValues(String column_list, String values, String target_table) throws ClassNotFoundException {
		StringTokenizer stoken = new StringTokenizer(values, "|");
		while (stoken.hasMoreElements()) {
			sql = "INSERT INTO STAGING." + target_table + "(" + column_list + ") VALUES " +  stoken.nextToken() ;
			System.out.println(sql);
			try {
				pst = DBConnection.ConnectStaging().prepareStatement(sql);
				pst.executeUpdate();
				
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		return true; 
		
	}

	public boolean insertLog(String table, String file_status, int config_id, String timestamp,
			String stagin_load_count, String file_name) throws ClassNotFoundException {
		sql = "INSERT INTO " + table
				+ "(config_id, file_name, state, staging_timestamp, download_timestamp,transform_timestamp,staging_count, transform_count) values(?,?,?,?,?,?,?,?)";
		try {
			pst = DBConnection.ConnectControl().prepareStatement(sql);
			pst.setString(1, file_name);
			pst.setInt(2, config_id);
			pst.setString(3, file_status);
			pst.setInt(4, Integer.parseInt(stagin_load_count));
			pst.setString(5, timestamp);
			pst.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
	public boolean updateLog(int config_id, String file_name, String state, Date staging_timestamp) throws ClassNotFoundException {
		Connection connection;
		try {
			connection = DBConnection.ConnectControl();
			PreparedStatement ps1 = connection.prepareStatement("UPDATE log SET active = 0 WHERE file_name=?");
			ps1.setString(1, file_name);
			ps1.executeUpdate();
			PreparedStatement ps = connection.prepareStatement("INSERT INTO log (config_id, file_name, file_type, status, file_timestamp, active) value (?,?,?,?,?,1)");
			ps.setInt(1, config_id);
			ps.setString(2, file_name);
			ps.setString(3, state);
			ps.setDate(4, staging_timestamp);
			ps.executeUpdate();
			connection.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean createTable(String table_name, String variables, String column_list) throws ClassNotFoundException {
		System.out.println("create");
		sql = "CREATE TABLE "+table_name+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
		String[] vari = variables.split(",");
		String[] col = column_list.split(",");
		for(int i =0;i<vari.length;i++) {
			sql+=col[i]+" "+vari[i]+ " NOT NULL,";
		}
		sql = sql.substring(0,sql.length()-1)+")";
		System.out.println(sql);
		try {
			pst = DBConnection.ConnectStaging().prepareStatement(sql);
			pst.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (pst != null)
					pst.close();
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		DatabaseMetaData dbm = util.DBConnection.ConnectStaging().getMetaData();
		ResultSet tables = dbm.getTables(null, null, "student", null);
		while(tables.next()) {
			System.out.println("???");
		}
	}
}
