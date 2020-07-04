package ETL;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import configuration.Config;
import util.ConfigUtils;
import util.DBConnection;
import util.LogUtils;

public class LoadFromStagingToWareHouse {
	// Chuyen du lieu tu staging den Warehouse
	Connection controlCon = null;
//	Connection controlCon = null;
//	Connection controlCon = null;
	
	
	
	public LoadFromStagingToWareHouse() 
			throws ClassNotFoundException, SQLException {
		this.controlCon = DBConnection.ConnectControl();
//		this.controlCon = DBConnection.ConnectStaging();
//		this.controlCon = DBConnection.ConnectWareHouse();
	}
	Timestamp nonValueDate = new Timestamp(new GregorianCalendar(1970,02,01).getTimeInMillis());
	public void load() throws SQLException, ClassNotFoundException {
		List<Integer> listConfig = LogUtils.getConfigByState("TR");
//		Connection conControl = null;
//		conControl = DBConnection.ConnectControl();
//		Connection controlConnection = DBConnection.ConnectWareHouse();
//		Connection conStaging = DBConnection.ConnectStaging();
		int insertedRecordCount = 0;
		for (int i = 0; i < listConfig.size(); i++) {
			
			Map<String,String> wh_config = getcontrolConfig(listConfig.get(i));
			String required_columns = wh_config.get("required_columns");
			String staging_table = wh_config.get("staging_table");
			String warehouse_table = wh_config.get("warehouse_table");
			String warehouse_columns = wh_config.get("warehouse_columns");
			String staging_naturalKey=wh_config.get("staging_naturalKey");
			String warehouse_naturalKey = wh_config.get("warehouse_naturalKey");
			ResultSet dataFromStaging = getDataFromStaging(staging_table, required_columns);
			while(dataFromStaging.next()) {
				String skey = dataFromStaging.getString(staging_naturalKey);
				if(checkExisted(skey, warehouse_table, warehouse_naturalKey)) {
					if(checkOverlap()) {
						updateOldRecord(warehouse_table, warehouse_naturalKey, staging_naturalKey);
					}
				}
				else {
					insertNewRecord(dataFromStaging, dataFromStaging.getRow(), required_columns, warehouse_columns, warehouse_table);
					insertedRecordCount++;
				}
			}
			Timestamp transform_timestamp = new Timestamp(System.currentTimeMillis());
			//Thay doi trang thai log cua tat ca cac file trong config
			LogUtils.updateNewState(listConfig.get(i), "SUC", transform_timestamp, insertedRecordCount);
			//Truncate table trong staging
			truncateStaging(staging_table);
			}
			
	}
	private void truncateStaging(String staging_table) throws SQLException, ClassNotFoundException {
		controlCon = DBConnection.ConnectStaging();
		controlCon.createStatement().execute("TRUNCATE TABLE STAGING." + staging_table);
	}
	private void insertNewRecord(ResultSet dataFromStaging, int row, String required_columns,
			String warehouse_columns, String warehouse_table) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		dataFromStaging.absolute(row);
		StringBuilder values = new StringBuilder();
		StringTokenizer stoken = new StringTokenizer(required_columns, ",");
		while(stoken.hasMoreElements()) {
			values.append("'");
			values.append(dataFromStaging.getString(stoken.nextToken()));
			values.append("',");
		}
		String insertToWarehouse = "insert into WAREHOUSE."+ warehouse_table + "(" + warehouse_columns + "," + "dt_expired)" +
				" values (" + values + "? )";
		System.out.println(insertToWarehouse);
		PreparedStatement ps = controlCon.prepareStatement(insertToWarehouse);
		ps.setTimestamp(1, nonValueDate);
		ps.execute();
		ps.close();
	}
	private ResultSet getDataFromStaging(String staging_table, String required_columns) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String selectFromStaging = "Select " + required_columns + " from STAGING." + staging_table;
		System.out.println(selectFromStaging);
		Statement st = controlCon.createStatement();
		ResultSet rs = st.executeQuery(selectFromStaging);
		return rs;
	}
	private void updateOldRecord(String warehouse_table, String warehouse_naturalkey, String staging_naturalKey) 
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String updateOldRecord = "Update WAREHOUSE."+ warehouse_table + " set dt_expired = ?'"+ timestamp +"' where " 
			    + warehouse_naturalkey  +" = " + staging_naturalKey + " and dt_expired = ? ";
		PreparedStatement ps = controlCon.prepareStatement(updateOldRecord);
		Long millis = System.currentTimeMillis();
		Timestamp currentTS = new Timestamp(millis);
		ps.setTimestamp(1, currentTS);
		ps.setTimestamp(2, nonValueDate);
		ps.execute();
		ps.close();
	}
	// kiem tra theo khoa tu nhien 
	// tra ve true neu ton tai trong warehouse 1 record tuong ung
	private boolean checkExisted(String sKey, String warehouse_table, String warehouse_naturalKey) 
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String checkQuery = "Select * from WAREHOUSE."+ warehouse_table + " where " + warehouse_naturalKey + " = "+ sKey;
		PreparedStatement ps = controlCon.prepareStatement(checkQuery);
		ResultSet rs = ps.executeQuery();
		if(rs.next()) {
			ps.close();
			return true;
		}
		ps.close();
		return false;
	}
	private Map<String,String> getcontrolConfig(int config_id) throws ClassNotFoundException, SQLException {
		Map<String, String> config = new HashMap<String, String>();
		String getcontrolConfig = "Select required_columns,staging_table,warehouse_table,warehouse_columns,staging_naturalKey,"
				+ "warehouse_naturalKey from CONTROL.warehouse_config"
				+ " where config_id = "+ config_id;
		PreparedStatement ps = controlCon.prepareStatement(getcontrolConfig);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
		config.put("required_columns", rs.getString("required_columns"));
		config.put("staging_table", rs.getString("staging_table"));
		config.put("warehouse_table",rs.getString("warehouse_table"));
		config.put("warehouse_columns", rs.getString("warehouse_columns"));
		config.put("staging_naturalKey", rs.getString("staging_naturalKey"));
		config.put("warehouse_columns", rs.getString("warehouse_columns"));
		}
		ps.close();
		return config;
	}
	public List<String> getNaturalKeyFromStaging(String staging_naturalKey, String staging_table) throws ClassNotFoundException, SQLException {
		String getKeysFromStaging = "select " + staging_naturalKey +" from STAGING." + staging_table;
		PreparedStatement ps = controlCon.prepareStatement(getKeysFromStaging);
		ResultSet rs = ps.executeQuery();
		List<String> naturalKeys = new ArrayList<String>();
		while(rs.next()) {
			naturalKeys.add(rs.getString("staging_naturalKey")) ;
		}
		return naturalKeys;
	}
	public boolean checkOverlap() {
		return false;
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		LoadFromStagingToWareHouse load = new LoadFromStagingToWareHouse();
	//	Config config = ConfigUtils.getConfig("f.txt");
		load.load();
	}

}
