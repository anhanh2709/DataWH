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

import com.mysql.cj.xdevapi.Type;

import java.sql.CallableStatement;



import configuration.Config;
import configuration.Log;
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
	
	
	
	public LoadFromStagingToWareHouse() 
			throws ClassNotFoundException, SQLException {
		// thực hiện kết nối đến database
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
		// khai báo biến đếm số dòng được chuyển từ staging sang warehouse
		int insertedRecordCount = 0;
		System.out.println("transfering......");
//		for (int i = 0; i < listConfig.size(); i++) {
			// Lấy các dữ liệu từ bảng warehouse config thông qua config được đưa vào
			Map<String,String> wh_config = getcontrolConfig(config.getConfig_id());
			// lấy các trường mà bảng trong warehouse yêu cầu
			String required_columns = wh_config.get("required_columns");
			// tên bảng được yêu cầu trong staging
			String staging_table = wh_config.get("staging_table");
			// lấy tên bảng được chỉ định trong database warehouse
			String warehouse_table = wh_config.get("warehouse_table");
			// lấy danh sách các trường mà dữ liệu sẽ được thêm vào trong warehouse
			String warehouse_columns = wh_config.get("warehouse_columns");
			// lấy tên trường dữ liệu mà làm khóa tự nhiên trong database staging
			String staging_naturalKey=wh_config.get("staging_naturalKey");
			// lấy tên trường dữ liệu mà làm khóa tự nhiên trong database warehouse
			String warehouse_naturalKey = wh_config.get("warehouse_naturalKey");
//			String constraints_flist = wh_config.get("constraints_flist");
			// lấy trường dữ liệu kiểu thời gian đã được quy định trong data feed quy định thời điểm dữ liệu hết hiệu lực
			// được thêm vào nếu đã quy đinh sẵn
			// nếu không sẽ sử dụng thời gian hiện tại để xác định ngày hết hạn
			String dt_expired = wh_config.get("dt_expired");
			// lấy danh sách các thủ tục trong database nhằm kiểm tra và chuyển đổi dữ liệu
			String constraints = wh_config.get("constraints");
			Date dt_expired_date = Date.valueOf(dt_expired);
		//	ResultSet dataFromStaging = getDataFromStaging(staging_table, required_columns);
			//lấy dữ liệu từ trong database staging, trả về 1 resultset chứa dữ liệu 
			getDataFromStaging(staging_table, required_columns);
			// chạy các hàm kiểm tra dữ liệu và chuyển đổi 
			checkConstraints(constraints);
			//System.out.println(dataFromStaging.);
			// kiểm tra dữ liện bên trong resultset còn hay không
			while(dataFromStaging.next()) {
				// lấy natural key của của bảng chứa dữ liêu trong staging 
				String skey = dataFromStaging.getString(staging_naturalKey);
				// kiểm tra xem đã tồn tại dòng dữ liệu về 1 đối tượng cụ thể trong warehouse hay chưa
				// thông qua natural key của cả 2 bảng 
				if(checkExisted(skey, warehouse_table, warehouse_naturalKey)) {
					// kiểm tra dữ liệu về đối tượng có trùng khớp hoàn toàn hay không
					// nếu có thì bỏ qua dòng record này
					if(checkOverlap(dataFromStaging.getString("id"), warehouse_table)) {
						// nếu dữ liêu về đối tượng trùng hoàn toàn
						// thêm 1 dòng dữ liệu mới vói dũ liệu từ resultset 
						insertNewRecord(dataFromStaging.getRow(), required_columns, warehouse_columns, warehouse_table);
						// update dòng dữ liệu cũ đang có hiệu lực thành hết hạn
						updateOldRecord(warehouse_table, warehouse_naturalKey, skey,dt_expired_date);
						// tăng biến đếm số dòng thêm vào thành công
						insertedRecordCount++;
					}
				}
				else {
					// thêm một dòng mới từ dữ liêu trong result Set
					insertNewRecord(dataFromStaging.getRow(), required_columns, warehouse_columns, warehouse_table);
					// tăng biến đếm
					insertedRecordCount++;
				}
				
			}
			// khai báo 1 biến thời gian với giá trị là thời gian hiện tại
			Timestamp transform_timestamp = new Timestamp(System.currentTimeMillis());
			//Thay doi trang thai log cua tat ca cac file trong config
			List<configuration.Log> listEXS = LogUtils.getConfigByState("EXS");
			for (configuration.Log log : listEXS) {
				if(log.getConfig_id() == config.getConfig_id()) {
					LogUtils.updateNewState(config.getConfig_id(), "SUC", transform_timestamp, insertedRecordCount,log.getFile_name());
				}
			}
//			LogUtils.updateNewState(config.getConfig_id(), "SUC", transform_timestamp, insertedRecordCount);
			System.out.println("transform success");
			//Truncate table trong staging
			truncateStaging(staging_table);
		//	mailUtils.SendMail("", "Chuyen du lieu thanh cong", "Chuyen du lieu config" + config.getConfig_name() + "thanh cong");
		//	}
			
	}
	// phương thức kiểm tra và chuyển đổi dữ liệu thông qua các thủ tục quy định sẵn
	private void checkConstraints(String constraints) throws SQLException {
	// TODO Auto-generated method stub
		if(constraints != null) {
			// tách trường constraints_flist ra thành các thủ tục kèm với tên trường mà thủ tục đó yêu cầu
		String[] procs = constraints.split(",");
		CallableStatement cstmt = null;
		// thực thi các thủ tục trong danh sách
		for (String proc : procs) {
			// tách tên thủ tục ra với tên trường mà thủ tục đó yêu cầu
			String[] procWithParam = proc.split("~");
			// tên thủ tục
			String procedure = procWithParam[0];
			// tên trường tham số
			String param  = procWithParam[1];
			String paramvalue = "";
			String returnvalue = "";
			int exists = -1;
			// kiểm tra từng dòng dữ liệu trong result set chứ data lấy lên từ databse staging
			while(dataFromStaging.next()) {
				// lấy dữ liệu của trường được yêu cầu
				paramvalue = dataFromStaging.getString(param);
				// gọi tên thủ tục trong database warehouse
				String call = "{call warehouse." +procedure+" (?,?,?)}";
				cstmt = controlCon.prepareCall(call);
				// đăng ký các biến đầu vào và đầu ra cho thủ tục
				cstmt.setString(1, paramvalue);
				// dữ liệu của truongf yêu cầu sau khi chuyển đổi
				cstmt.registerOutParameter(2, Types.VARCHAR);
				// kết quả xem có tồn tại dữ liệu liên quan đến giá trị đầu vào hay k
				cstmt.registerOutParameter(3, Types.TINYINT);
				// thực thi thủ tục
				cstmt.execute();
				returnvalue = cstmt.getString(2);
				exists = cstmt.getInt(3);
				// nếu dữ liệu yêu cầu của khóa tự nhiên không tồn tại thì record bị bỏ đi
				if(exists == 0) {
					dataFromStaging.deleteRow();
				}
				else if(returnvalue != null) {
				// nếu có: cập nhập mới dữ liệu của trường yêu cầu 
					dataFromStaging.updateString(param, returnvalue);
					dataFromStaging.updateRow();
				}
				else {
					continue;
				}
			}
			// di chuyển con trỏ của result set về lại dòng bắt đầu
			dataFromStaging.absolute(0);
		}
		}
}
	private void truncateStaging(String staging_table) throws SQLException, ClassNotFoundException {
		//controlCon = DBConnection.ConnectStaging();
		controlCon.createStatement().execute("TRUNCATE TABLE STAGING." + staging_table);
	}
	private void insertNewRecord(int row, String required_columns,
			String warehouse_columns, String warehouse_table) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		// di chuyển con trỏ của result set về hàng chứa record cần thêm vào
		dataFromStaging.absolute(row);
		StringBuilder values = new StringBuilder();
		// tách trường chứa tên các cột mà database warehouse yêu cầu phân tách nhau bởi dấu , 
		StringTokenizer stoken = new StringTokenizer(required_columns, ",");
		// xây dựng câu lệnh sql
		while(stoken.hasMoreElements()) {
			values.append("'");
			values.append(dataFromStaging.getString(stoken.nextToken()));
			values.append("',");
		}
		Long millis = System.currentTimeMillis();
		Date currentDate = new Date(millis);
		String insertToWarehouse = "insert into WAREHOUSE."+ warehouse_table + "(" + warehouse_columns + "," + "dt_expired,dt_lastchange,isActive)" +
				" values (" + values + "'9999-12-31',?,?)";
		System.out.println(insertToWarehouse);
		PreparedStatement ps = controlCon.prepareStatement(insertToWarehouse);
	//	ps.setDate(1, maxDate);
		ps.setDate(1, currentDate);
		ps.setBoolean(2, true);
		ps.execute();
		ps.close();
	}
	private void getDataFromStaging(String staging_table, String required_columns) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		String selectFromStaging = "Select id," + required_columns + " from STAGING." + staging_table;
		Statement st = controlCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
		dataFromStaging = st.executeQuery(selectFromStaging);
		return;
	}
	private void updateOldRecord(String warehouse_table, String warehouse_naturalkey, String staging_naturalKey, Date dt_expired_date) 
			throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		//Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String updateOldRecord = "Update WAREHOUSE."+ warehouse_table + " set dt_expired = ?,dt_lastchange = ?,isActive = 0"+" where " 
			    + warehouse_naturalkey  +" = " + staging_naturalKey + " and isActive = 1";
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
		String checkQuery = "Select "+warehouse_naturalKey+ 
				" from WAREHOUSE."+ warehouse_table + " where " + warehouse_naturalKey + " = "+ sKey;
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
				+ "warehouse_naturalKey,dt_expired,constraint_flist from CONTROL.warehouse_config "
				+ "where config_id = "+ config_id;
		PreparedStatement ps = controlCon.prepareStatement(getcontrolConfig);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
		config.put("required_columns", rs.getString("required_columns"));
		config.put("staging_table", rs.getString("staging_table"));
		config.put("warehouse_table",rs.getString("warehouse_table"));
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
	public List<String> getNaturalKeyFromStaging(String staging_naturalKey, String staging_table) throws ClassNotFoundException, SQLException {
		String getKeysFromStaging = "select " + staging_naturalKey +" from STAGING." + staging_table;
		PreparedStatement ps = controlCon.prepareStatement(getKeysFromStaging);
		ResultSet rs = ps.executeQuery();
		List<String> naturalKeys = new ArrayList<String>();
		while(rs.next()) {
			naturalKeys.add(rs.getString("staging_naturalKey"));
		}
		return naturalKeys;
	}
	public boolean checkOverlap(String stagingId,String warehouseTable) throws SQLException, ClassNotFoundException {
			Connection con = DBConnection.ConnectControl();
			String overlapProcedure = "{Call warehouse."+warehouseTable +"_Overlap(?,?)}";
			CallableStatement cstmt = con.prepareCall(overlapProcedure);
			int overlap = 0;
			cstmt.setInt(1, Integer.parseInt(stagingId));
			cstmt.registerOutParameter(2, Types.TINYINT);
			cstmt.execute();
			overlap = cstmt.getInt(2);
			//con.close();
			if(overlap == 1) 
				return true;
			else
				return false;
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
