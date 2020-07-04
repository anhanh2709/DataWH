package configuration;

import java.sql.SQLException;

import util.ConfigUtils;

public class Config {
	private int config_id;
	private String config_name;
	private String file_type;
	private String src_url;
	private String src_path;
	private String src_user;
	private String src_pass;
	private String import_dir;
	private String success_dir;
	private String err_dir;
	private String columnList;
	private String delimeter;
	private String dataTypes;
	private String target_tb;
	private String file_Mask;

	public int getConfig_id() {
		return config_id;
	}

	public void setConfig_id(int config_id) {
		this.config_id = config_id;
	}

	public String getConfig_name() {
		return config_name;
	}

	public void setConfig_name(String config_name) {
		this.config_name = config_name;
	}

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public String getSrc_url() {
		return src_url;
	}

	public void setSrc_url(String src_url) {
		this.src_url = src_url;
	}

	public String getSrc_path() {
		return src_path;
	}

	public void setSrc_path(String src_path) {
		this.src_path = src_path;
	}

	public String getSrc_user() {
		return src_user;
	}

	public void setSrc_user(String src_user) {
		this.src_user = src_user;
	}

	public String getSrc_pass() {
		return src_pass;
	}

	public void setSrc_pass(String src_pass) {
		this.src_pass = src_pass;
	}

	public String getImport_dir() {
		return import_dir;
	}

	public void setImport_dir(String import_dir) {
		this.import_dir = import_dir;
	}

	public String getSuccess_dir() {
		return success_dir;
	}

	public void setSuccess_dir(String success_dir) {
		this.success_dir = success_dir;
	}

	public String getErr_dir() {
		return err_dir;
	}

	public void setErr_dir(String err_dir) {
		this.err_dir = err_dir;
	}

	public String getColumnList() {
		return columnList;
	}

	public void setColumnList(String columnList) {
		this.columnList = columnList;
	}

	public String getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(String delimeter) {
		this.delimeter = delimeter;
	}

	public String getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(String dataTypes) {
		this.dataTypes = dataTypes;
	}

	public String getTarget_tb() {
		return target_tb;
	}

	public void setTarget_tb(String target_tb) {
		this.target_tb = target_tb;
	}

	public String getFile_Mask() {
		return file_Mask;
	}

	public void setFile_Mask(String file_Mask) {
		this.file_Mask = file_Mask;
	}

	@Override
	public String toString() {
		return "Config [config_id=" + config_id + ", config_name=" + config_name + ", file_type=" + file_type
				+ ", src_url=" + src_url + ", src_path=" + src_path + ", src_user=" + src_user + ", src_pass="
				+ src_pass + ", import_dir=" + import_dir + ", success_dir=" + success_dir + ", err_dir=" + err_dir
				+ ", columnList=" + columnList + ", delimeter=" + delimeter + ", dataTypes=" + dataTypes
				+ ", target_tb=" + target_tb + ", file_Mask=" + file_Mask + "]";
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Config config = ConfigUtils.getConfig("f.txt");
		System.out.println(config.toString());
	}
}
