package ETL;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import util.LogUtils;

import configuration.Config;
import configuration.Log;
import dao.ControlDatabase;
import mail.mailUtils;
import model.ReadFile;
import util.ConfigUtils;
import util.DBConnection;

public class LoadFromLocalToStaging {

	private String configName;

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public void ExtractToDatabase(ReadFile rf, String file_arg) throws ClassNotFoundException, SQLException {
//		1. Kết nối với database control
//		2. Lấy 1 dòng trong config
		Config config = ConfigUtils.getConfig(this.configName);
//		3. Lấy các trường trong config
		String target_tb = config.getTarget_tb();
		String file_type = config.getFile_type();
		String success_dir = config.getSuccess_dir();
		String delimeter = config.getDelimeter();
		String columnList = config.getColumnList();
		String dataTypes = config.getDataTypes();
//		4. Kết nối với database staging
		DBConnection dbconnect = new DBConnection();
		Connection connect = dbconnect.ConnectStaging();
//		5. Kiểm tra table tồn tại chưa
//		Chưa thì tạo
		if (!rf.getCdb().tableExist(target_tb)) {
			rf.getCdb().createTable(target_tb, dataTypes, columnList);
		}
//		Nếu tạo rồi thì truncate table 
		else {
			rf.getCdb().truncateTable(connect, target_tb);
		}

//		mở file trong success_dir
		File suc_dir = new File(success_dir);
//		6. Kiểm tra xem success_có tồn tại
		if (suc_dir.exists()) {
			String extention = "";
//			7. Lấy trong log list file có trạng thái là ER
			List<Log> listlog = LogUtils.getConfigByState("ER");
//			lấy ra các cột cách nhau bởi dấu phẩy
			StringTokenizer str = new StringTokenizer(columnList, ",");
//			8. Kiểm tra file có trong success_dir không
			File file = new File(suc_dir + File.separator + file_arg);
			System.out.println(file);

//			File không tồn tại trong success_dir
			if (!file.exists()) {
				System.out.println("file" + file.getName() + "not exists in success directory");
				mailUtils.SendMail("kanh2709@gmail.com", "Load file từ local lên staging",
						"File không tồn tại trong folder!!!");
				return;
			}

//			Lấy log
			for (Log log : listlog) {
//				Lấy tên file trong dir
				String file_name = file.getName();
//				9. Kiểm tra file có trong log không
				if (file_name.equals(log.getFile_name())) {
					String values = "";
//						10. Kiểm tra đuôi file xem là loại nào
					if (file.getPath().endsWith(".txt")) {
//						11. Đọc file
						values = rf.readValuesTXT(file, str.countTokens());
					}
					if (file.getPath().endsWith(".csv")) {
						values = rf.readValuesTXT(file, str.countTokens());
					} else if (file.getPath().endsWith(".xlsx")) {
						values = rf.readValuesXLSX(file, str.countTokens());
					}
					System.out.println(values);
//						12. Kiểm tra dữ liệu
					if (values != null) {

						String state;
						int config_id = config.getConfig_id();

//						time
//						currentTimeMillis lấy thời gian
						Long millis = System.currentTimeMillis();
//						timestamp thư viện của sql
						Timestamp currentTS = new Timestamp(millis);

//						count line
						String staging_count = "";
						try {
							staging_count = countLines(file, extention) + "";
						} catch (InvalidFormatException
								| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
							e.printStackTrace();
						}

						System.out.println("Extracting........");
//							13. Ghi dữ liệu vào table
						if (rf.writeDataToBD(columnList, target_tb, values)) {
//								Thông báo thành công
							System.out.println("Extract success");
							state = "EXS";
//								Ghi log
							LogUtils.updateStateForAFile(config_id, state, currentTS, Integer.parseInt(staging_count),
									file_name);

						} else {
							state = "EXF";
//								Thông báo thành công
							System.out.println("Extract Fail");
//								Ghi log
							LogUtils.updateStateForAFile(config_id, state, currentTS, Integer.parseInt(staging_count),
									file_name);
//								Gửi mail
							mailUtils.SendMail("kanh2709@gmail.com", "Load file từ local lên staging",
									"Load thất bại!!!");
						}
					}
				}

			}
		} else {
//			Folder success không tồn tại
			System.out.println("Path not exists!!!");

			mailUtils.SendMail("kanh2709@gmail.com", "Load file từ local lên staging", "folder không tồn tại!!!");
			return;
		}
	}

	private int countLines(File file, String extention)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(".txt") != -1 || extention.indexOf(".csv") != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line;
//				đọc từng dòng
				while ((line = bReader.readLine()) != null) {
					if (!line.trim().isEmpty()) {
						result++;
					}
				}
				bReader.close();
			} else if (extention.indexOf(".xlsx") != -1) {
				workBooks = new XSSFWorkbook(file);
				XSSFSheet sheet = workBooks.getSheetAt(0);
				Iterator<Row> rows = sheet.iterator();
				rows.next();
				while (rows.hasNext()) {
					rows.next();
					result++;
				}
				return result;
			}

		} catch (IOException | org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
			e.printStackTrace();
		} finally {
			if (workBooks != null) {
				try {
					workBooks.close();
				} catch (IOException e) {
					// ghi lỗi
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	// chạy hết các file có state là EXF
	public void reExtract() throws ClassNotFoundException, SQLException {
		List<Log> listLog = LogUtils.getConfigByState("EXF");
		for (Log log : listLog) {
			ReadFile rf = new ReadFile();
			this.ExtractToDatabase(rf, log.getFile_name());
		}
	}

	// chạy từng file có state là EXF
	public void reExtract(String config_name) throws ClassNotFoundException, SQLException {
		List<Log> listLog = LogUtils.getConfigByState("EXF");
		for (Log log : listLog) {
			if (log.getConfig_id() == ConfigUtils.getConfig(config_name).getConfig_id()) {
				ReadFile rf = new ReadFile();
				this.ExtractToDatabase(rf, log.getFile_name());
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		ls.setConfigName("f_sinhvien");
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		ls.ExtractToDatabase(rf, "sinhvien_chieu_nhom13.xlsx");
	}
}
