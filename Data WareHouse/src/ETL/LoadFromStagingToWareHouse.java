package ETL;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

//import com.mysql.cj.xdevapi.Type;

import java.sql.CallableStatement;

import configuration.Config;
import mail.mailUtils;
import util.ConfigUtils;
import util.DBConnection;
import util.LogUtils;

public class LoadFromStagingToWareHouse {
	// Chuyen du lieu tu staging den Warehouse
	Connection controlCon = null;
	ResultSet dataFromStaging = null;
//	Connection controlCon = null;
//	Connection controlCon = null;

	public LoadFromStagingToWareHouse() throws ClassNotFoundException, SQLException {
		this.controlCon = DBConnection.ConnectControl();
//		this.controlCon = DBConnection.ConnectStaging();
//		this.controlCon = DBConnection.ConnectWareHouse();
	}

//	Date nonValueDate = new Date(new GregorianCalendar(9999,31,12).getTimeInMillis());
	public void load(Config config) throws SQLException, ClassNotFoundException {
//		List<Integer> listConfig = LogUtils.getConfigIDByState("EXS");
//		Connection conControl = null;
//		conControl = DBConnection.ConnectControl();
//		Connection controlConnection = DBConnection.ConnectWareHouse();
//		Connection conStaging = DBConnection.ConnectStaging();
		int insertedRecordCount = 0;
//		for (int i = 0; i < listConfig.size(); i++) {
		Map<String, String> wh_config = getcontrolConfig(config.getConfig_id());
		String required_columns = wh_config.get("required_columns");
		String staging_table = wh_config.get("staging_table");
		String warehouse_table = wh_config.get("warehouse_table");
		String warehouse_columns = wh_config.get("warehouse_columns");
		String staging_naturalKey = wh_config.get("staging_naturalKey");
		String warehouse_naturalKey = wh_config.get("warehouse_naturalKey");
//			String constraints_flist = wh_config.get("constraints_flist");
		String dt_expired = wh_config.get("dt_expired");
		String constraints = wh_config.get("constraints");
		Date dt_expired_date = Date.valueOf(dt_expired);
		// ResultSet dataFromStaging = getDataFromStaging(staging_table,
		// required_columns);
		getDataFromStaging(staging_table, required_columns);
		checkConstraints(constraints);
		// System.out.println(dataFromStaging.);
		while (dataFromStaging.next()) {
			String skey = dataFromStaging.getString(staging_naturalKey);
			if (checkExisted(skey, warehouse_table, warehouse_naturalKey)) {
				if (checkOverlap()) {
					insertNewRecord(dataFromStaging.getRow(), required_columns, warehouse_columns, warehouse_table);
					updateOldRecord(warehouse_table, warehouse_naturalKey, skey, dt_expired_date);
				}
			} else {
				insertNewRecord(dataFromStaging.getRow(), required_columns, warehouse_columns, warehouse_table);
				insertedRecordCount++;
			}

		}
		Timestamp transform_timestamp = new Timestamp(System.currentTimeMillis());
		// Thay doi trang thai log cua tat ca cac file trong config
		LogUtils.updateNewState(config.getConfig_id(), "SUC", transform_timestamp, insertedRecordCount);
		// Truncate table trong staging
		// truncateStaging(staging_table);
		mailUtils.SendMail("", "Chuyen du lieu thanh cong",
				"Chuyen du lieu config" + config.getConfig_name() + "thanh cong");
		// }

	}

	private void checkConstraints(String constraints) throws SQLException {
		// TODO Auto-generated method stub
		String[] procs = constraints.split(",");
		CallableStatement cstmt = null;

		for (String proc : procs) {
			String[] procWithParam = proc.split("~");
			String procedure = procWithParam[0];
			String param = procWithParam[1];
			System.out.println(param);
			String paramvalue = "";
			String returnvalue = "";
			int exists = -1;
			while (dataFromStaging.next()) {
				paramvalue = dataFromStaging.getString(param);
				String call = "{call warehouse." + procedure + " (?,?,?)}";
				System.out.println("call " + call);
				cstmt = controlCon.prepareCall(call);
				System.out.println("param value" + paramvalue);
				cstmt.setString(1, paramvalue);
				cstmt.registerOutParameter(2, Types.VARCHAR);
				cstmt.registerOutParameter(3, Types.TINYINT);
				cstmt.execute();
				returnvalue = cstmt.getString(2);
				exists = cstmt.getInt(3);
				System.out.println(returnvalue);
				if (exists == 0) {
					System.out.println("???");
					dataFromStaging.deleteRow();
				} else {
					System.out.println("vcl");
					dataFromStaging.updateString(param, returnvalue);
					dataFromStaging.updateRow();
				}
			}
			dataFromStaging.absolute(0);
		}
	}

	private void truncateStaging(String staging_table) throws SQLException, ClassNotFoundException {
		// controlCon = DBConnection.ConnectStaging();
		controlCon.createStatement().execute("TRUNCATE TABLE STAGING." + staging_table);
	}

	private void insertNewRecord(int row, String required_columns, String warehouse_columns, String warehouse_table)
			throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		dataFromStaging.absolute(row);
		StringBuilder values = new StringBuilder();
		StringTokenizer stoken = new StringTokenizer(required_columns, ",");
		while (stoken.hasMoreElements()) {
			values.append("'");
			values.append(dataFromStaging.getString(stoken.nextToken()));
			values.append("',");
		}
		Long millis = System.currentTimeMillis();
		Date currentDate = new Date(millis);
		String insertToWarehouse = "insert into WAREHOUSE." + warehouse_table + "(" + warehouse_columns + ","
				+ "dt_expired,dt_lastchange,isActive)" + " values (" + values + "'9999-12-31',?,?)";
		System.out.println(insertToWarehouse);
		PreparedStatement ps = controlCon.prepareStatement(insertToWarehouse);
		// ps.setDate(1, maxDate);
		ps.setDate(1, currentDate);
		ps.setBoolean(2, true);
		ps.execute();
		ps.close();
	}

	private void getDataFromStaging(String staging_table, String required_columns)
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String selectFromStaging = "Select id," + required_columns + " from STAGING." + staging_table;
		Statement st = controlCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		dataFromStaging = st.executeQuery(selectFromStaging);
		return;
	}

	private void updateOldRecord(String warehouse_table, String warehouse_naturalkey, String staging_naturalKey,
			Date dt_expired_date) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		// Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String updateOldRecord = "Update WAREHOUSE." + warehouse_table
				+ " set dt_expired = ?,dt_lastchange = ?,isActive = 0" + " where " + warehouse_naturalkey + " = "
				+ staging_naturalKey + " and isActive = 1";
		PreparedStatement ps = controlCon.prepareStatement(updateOldRecord);
		Long millis = System.currentTimeMillis();
		Date currentDate = new Date(millis);
		ps.setDate(1, dt_expired_date);
		ps.setDate(2, currentDate);
		ps.execute();
		ps.close();
	}

	// kiem tra theo khoa tu nhien
	// tra ve true neu ton tai trong warehouse 1 record tuong ung
	private boolean checkExisted(String sKey, String warehouse_table, String warehouse_naturalKey)
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String checkQuery = "Select * from WAREHOUSE." + warehouse_table + " where " + warehouse_naturalKey + " = "
				+ sKey;
		System.out.println(checkQuery);
		PreparedStatement ps = controlCon.prepareStatement(checkQuery);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			ps.close();
			return true;
		}
		ps.close();
		return false;
	}

	private Map<String, String> getcontrolConfig(int config_id) throws ClassNotFoundException, SQLException {
		Map<String, String> config = new HashMap<String, String>();
		String getcontrolConfig = "Select required_columns,staging_table,warehouse_table,warehouse_columns,staging_naturalKey,"
				+ "warehouse_naturalKey,dt_expired,constraint_flist from CONTROL.warehouse_config "
				+ "where config_id = " + config_id;
		PreparedStatement ps = controlCon.prepareStatement(getcontrolConfig);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			config.put("required_columns", rs.getString("required_columns"));
			config.put("staging_table", rs.getString("staging_table"));
			config.put("warehouse_table", rs.getString("warehouse_table"));
			config.put("warehouse_columns", rs.getString("warehouse_columns"));
			config.put("staging_naturalKey", rs.getString("staging_naturalKey"));
			config.put("warehouse_columns", rs.getString("warehouse_columns"));
			config.put("warehouse_naturalKey", rs.getString("warehouse_naturalKey"));
			config.put("dt_expired", rs.getString("dt_expired"));
			config.put("constraints", rs.getString("constraint_flist"));
		}
		ps.close();
		return config;
	}

	public List<String> getNaturalKeyFromStaging(String staging_naturalKey, String staging_table)
			throws ClassNotFoundException, SQLException {
		String getKeysFromStaging = "select " + staging_naturalKey + " from STAGING." + staging_table;
		PreparedStatement ps = controlCon.prepareStatement(getKeysFromStaging);
		ResultSet rs = ps.executeQuery();
		List<String> naturalKeys = new ArrayList<String>();
		while (rs.next()) {
			naturalKeys.add(rs.getString("staging_naturalKey"));
		}
		return naturalKeys;
	}

	public boolean checkOverlap() {
		return true;
	}

	public void loadDate_dim() {
		String insertDateDim = "Insert into WAREHOUSE.Datedim(id, date_value) select col1, col2 from Staging.Datedim";

		try {
			Connection con = DBConnection.ConnectControl();
			con.prepareStatement(insertDateDim).execute();
			con.close();
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		LoadFromStagingToWareHouse load = new LoadFromStagingToWareHouse();
//		load.loadDate_dim();
		Config config = ConfigUtils.getConfig("f_dangky");
		load.load(config);

	}

}
