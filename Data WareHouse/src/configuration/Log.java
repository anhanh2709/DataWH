package configuration;

import java.sql.Timestamp;

public class Log {
	private int config_id;
	private String file_name;
	private String state;
	private Timestamp staging_timestamp;
	private Timestamp download_timestamp;
	private Timestamp transform_timestamp;
	private int staging_count;
	private int transform_count;

	public Log() {

	}

	public Log(int config_id, String file_name, String state, int staging_count, int transform_count) {
		this.config_id = config_id;
		this.file_name = file_name;
		this.state = state;
		this.staging_count = staging_count;
		this.transform_count = transform_count;
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

	public Timestamp getStaging_timestamp() {
		return staging_timestamp;
	}

	public void setStaging_timestamp(Timestamp staging_timestamp) {
		this.staging_timestamp = staging_timestamp;
	}

	public Timestamp getDownload_timestamp() {
		return download_timestamp;
	}

	public void setDownload_timestamp(Timestamp download_timestamp) {
		this.download_timestamp = download_timestamp;
	}

	public Timestamp getTransform_timestamp() {
		return transform_timestamp;
	}

	public void setTransform_timestamp(Timestamp transform_timestamp) {
		this.transform_timestamp = transform_timestamp;
	}

	public int getStaging_count() {
		return staging_count;
	}

	public void setStaging_count(int staging_count) {
		this.staging_count = staging_count;
	}

	public int getTransform_count() {
		return transform_count;
	}

	public void setTransform_count(int transform_count) {
		this.transform_count = transform_count;
	}

	@Override
	public String toString() {
		return "Log [config_id=" + config_id + ", file_name=" + file_name + ", state=" + state + ", staging_timestamp="
				+ staging_timestamp + ", download_timestamp=" + download_timestamp + ", transform_timestamp="
				+ transform_timestamp + ", staging_count=" + staging_count + ", transform_count=" + transform_count
				+ "]";
	}

}
