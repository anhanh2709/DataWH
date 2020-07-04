package ETL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import configuration.Config;
import configuration.Log;
import util.DBConnection;
import util.LogUtils;

public class LoadFromStagingToWareHouse {
	// Chuyen du lieu tu staging den Warehouse
	public void load(Config config) throws SQLException, ClassNotFoundException {
		List<Log> listConfig = LogUtils.getConfigByState("ESUC");
		Connection conControl = null;
		conControl = DBConnection.ConnectControl();
		Connection warehouseConnection = DBConnection.ConnectWareHouse();
		Connection conStaging = DBConnection.ConnectStaging();
		int insertedRecordCount = 0;
		for (int i = 0; i < listConfig.size(); i++) {
			String getWarehouseConfig = "Select required_columns,staging_table,warehouse_table,warehouse_columns,naturalKey from warehouse_config"
					+ " where config_id = "+ listConfig.get(i);
			PreparedStatement ps = conControl.prepareStatement(getWarehouseConfig);
			ResultSet rs = ps.executeQuery();
			String required_columns = "", staging_table = "", warehouse_table = "", warehouse_columns = "",
					staging_naturalKey="", warehouse_naturalkey = "";
			while(rs.next()) {
				required_columns = rs.getString("required_column");
				staging_table = rs.getString("staging_table");
				warehouse_table = rs.getString("warehouse_table");
				warehouse_columns = rs.getString("warehouse_columns");
				staging_naturalKey = rs.getString("staging_naturalKey");
				warehouse_naturalkey = rs.getString("warehouse_naturalKey");
			}
			
			String getKeysFromStaging = "select " + staging_naturalKey +"from " + staging_table;
			ps = conStaging.prepareStatement(getKeysFromStaging);
			rs = ps.executeQuery();
			while(rs.next()) {
				String skey = rs.getString(staging_naturalKey);
				if(checkExisted(skey, warehouse_table, warehouse_naturalkey)) {
					// cap nhap record da ton tai 
					String updateOldRecord = "Update "+ warehouse_table + " t1 set dt_expired = now() where " 
					    + warehouse_naturalkey  +" = " + skey + " and dt_expired = (Select MAX (dt_expired) from +" +warehouse_table 
						+ "t2 where t2." + warehouse_naturalkey + " = t1." + warehouse_naturalkey
						+ " group by + ("+ warehouse_naturalkey + ")";
					
					Statement stm = warehouseConnection.createStatement();
					stm.execute(updateOldRecord);
					stm.close();
				}
				String insertNewRecord = "insert into "+ warehouse_table +"("+ warehouse_columns + ")" +  
				" SELECT " + required_columns + " FROM STAGING." + staging_table;
				Statement stm = warehouseConnection.createStatement();
				try {
					stm.executeUpdate(insertNewRecord);
				} catch (Exception e) {
					System.out.println(e.getMessage());
				}
				finally {
					insertedRecordCount++;
				}
			}
			//Thay doi trang thai log cua tat ca cac file trong config
			Timestamp transform_timestamp = new Timestamp(System.currentTimeMillis());
			LogUtils.updateNewState(config.getConfig_id(), "SUC", transform_timestamp, insertedRecordCount);
			//Truncate table trong staging
			conStaging.createStatement().execute("TRUNCATE TABLE " + staging_table);
		}
	}
	// kiem tra theo khoa tu nhien 
	// tra ve true neu ton tai trong warehouse 1 record tuong ung
	private boolean checkExisted(String sKey, String warehouse_table, String warehouse_naturalKey) 
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Connection con = DBConnection.ConnectWareHouse();
		String checkQuery = "Select * from "+ warehouse_table + " where "+warehouse_naturalKey +" = "+ sKey;
		PreparedStatement ps = con.prepareStatement(checkQuery);
		ResultSet rs = ps.executeQuery();
		if(rs.next()) {
			ps.close();
			return true;
		}
		ps.close();
		return false;
	}

}
