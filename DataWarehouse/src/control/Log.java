package control;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import connection.DBConnection;

public class Log {
	private int config_id;
	private String file_name;
	private String state;
	private Date staging_timestamp;
	private Date download_timestamp;
	private Date transform_timestamp;
	private String staging_count;
	private String transform_count;
	private String action;
	private int active;

	public Log() {

	}

	public Log(int config_id, String file_name, String state, String staging_count, String transform_count,
			String action, int active) {
		this.config_id = config_id;
		this.file_name = file_name;
		this.state = state;
		this.staging_count = staging_count;
		this.transform_count = transform_count;
		this.action = action;
		this.active = active;
	}

	public List<Log> getLogsWithStatus(String condition) {
		List<Log> list = new ArrayList<>();
		PreparedStatement pst = null;
		ResultSet rs = null;
		String sql = "SELECT * FROM log where action = ?";
		Connection conn;
		try {
			conn = DBConnection.getConnection("control");
			pst = conn.prepareStatement(sql);
			pst.setString(1, condition);
			rs = pst.executeQuery();
			while (rs.next()) {
				list.add(new Log(rs.getInt("config_id"), 
						rs.getString("file_name"), 
						rs.getString("state"),
						rs.getString("staging_count"),
						rs.getString("transform_count"), 
						rs.getString("action"),
						rs.getInt("active")));
			}
		} catch (Exception e) {
			e.printStackTrace();
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
		return list;
	}

	public boolean insertLog(int configID, String fileName, String fileType, String status, String fileTimeStamp) {
		Connection connection;
		try {
			connection = DBConnection.getConnection("control");
			PreparedStatement ps1 = connection.prepareStatement("UPDATE log SET active=0 WHERE file_name=?");
			ps1.setString(1, fileName);
			ps1.executeUpdate();
			PreparedStatement ps = connection.prepareStatement(
					"INSERT INTO log (config_id, file_name, state,staging_timestamp,download_timestamp, transform_timestamp,staging_count, transform_count, active) value (?,?,?,?,?,1)");
			ps.setInt(1, configID);
			ps.setString(2, fileName);
			ps.setString(3, fileName.substring(fileName.indexOf('.') + 1));
			ps.setString(4, status);
			ps.setString(5, fileTimeStamp);
			ps.executeUpdate();
			connection.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public MyFile getFileWithStatus(String status) {
		MyFile file = new MyFile();
		Connection connection;
		try {
			connection = DBConnection.getConnection("control");
			PreparedStatement ps = connection
					.prepareStatement("SELECT file_name, file_type FROM data_file WHERE action=? AND active=1");
			ps.setString(1, status);
			ResultSet rs = ps.executeQuery();
			rs.last();
			if (rs.getRow() >= 1) {
				rs.first();
				file.setFileName(rs.getString("file_name"));
				file.setFileType(rs.getString("file_type"));
			} else {
				return null;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return file;
	}

	public static void main(String[] args) {
//		List<Log> list = getLogsWithStatus("ER");
//		for (Log log : list) {
//			System.out.println(log.toString());
//		}
//		if (log.getFileWithStatus("ER")!=null) {
//			System.out.println(log.getFileWithStatus("ER").toString());
//		} else {
//			System.out.println("No file status like that");
//		}

	}

	public int getConfig_id() {
		return config_id;
	}

	public void setConfig_id(int config_id) {
		this.config_id = config_id;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Date getStaging_timestamp() {
		return staging_timestamp;
	}

	public void setStaging_timestamp(Date staging_timestamp) {
		this.staging_timestamp = staging_timestamp;
	}

	public Date getDownload_timestamp() {
		return download_timestamp;
	}

	public void setDownload_timestamp(Date download_timestamp) {
		this.download_timestamp = download_timestamp;
	}

	public Date getTransform_timestamp() {
		return transform_timestamp;
	}

	public void setTransform_timestamp(Date transform_timestamp) {
		this.transform_timestamp = transform_timestamp;
	}

	public String getStaging_count() {
		return staging_count;
	}

	public void setStaging_count(String staging_count) {
		this.staging_count = staging_count;
	}

	public String getTransform_count() {
		return transform_count;
	}

	public void setTransform_count(String transform_count) {
		this.transform_count = transform_count;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public int getActive() {
		return active;
	}

	public void setActive(int active) {
		this.active = active;
	}

	@Override
	public String toString() {
		return "Log [config_id=" + config_id + ", file_name=" + file_name + ", state=" + state + ", staging_timestamp="
				+ staging_timestamp + ", download_timestamp=" + download_timestamp + ", transform_timestamp="
				+ transform_timestamp + ", staging_count=" + staging_count + ", transform_count=" + transform_count
				+ ", action=" + action + ", active=" + active + "]";
	}

}
