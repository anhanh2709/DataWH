package model;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import control.Config;
import control.Log;
import dao.ControlDatabase;

public class DataStaging {
	private String config_name;
	private String action;

	public String getConfig_name() {
		return config_name;
	}

	public void setConfig_name(String config_name) {
		this.config_name = config_name;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		DataStaging dw = new DataStaging();
		dw.setConfig_name("f.txt");
		dw.setAction("ER");
		DataProcess dp = new DataProcess();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		dp.setCdb(cdb);
		dw.ExtractToDB(dp);
	}

	public void ExtractToDB(DataProcess dp) throws ClassNotFoundException, SQLException {
		Config config = new Config(config_name);
		String target_tb = config.getTarget_tb();
		String file_type = config.getFile_type();
		String import_dir = config.getImport_dir();
		String delimeter = config.getDelimeter();
		String columnList = config.getColumnList();
		String dataTypes = config.getDataTypes();
		System.out.println(target_tb);

		if (!dp.getCdb().tableExist(target_tb)) {
			System.out.println(dataTypes);
			dp.getCdb().createTable(target_tb, dataTypes, columnList);
		}

		// File imp_dir = new File(import_dir);
		// if (imp_dir.exists()) {
		// // String extention = "";
		// // Log a = new Log();
		// List<Log> list = a.getLogsWithStatus("ER");
		// for (Log log : list) {
		// File file = new File(import_dir + "\\" + log.getFileName());
		// if (file.exists() && log.getFileStatus().equals("OK") &&
		// log.getActive() == 1) {
		// System.out.println(file.getName());
		// }
		// }
		File imp_dir = new File(import_dir);
		if (imp_dir.exists()) {
			String extention = "";
			Log a = new Log();
			List<Log> listLog = a.getLogsWithStatus("ER");
			File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				for (Log log : listLog) {
					String file_name = file.getName().replaceAll(file_type, "");
					if (file_name.equals(log.getFile_name()) && log.getState().equals("OK")
							&& log.getActive() == 1) {
						System.out.println(file.getName());
						if (file.getName().indexOf(file_type) != -1) {
							System.out.println(7);
							String values = "";
							if (file_type.equals(".txt")) {
								values = dp.readValuesTXT(file, delimeter);
								extention = ".txt";
							} else if (file_type.equals(".xlsx")) {
								values = dp.readValuesXLSX(file);
								extention = ".xlsx";
							}
							if (values != null) {
								String table = "data_file";
								String file_status;
								int config_id = config.getConfig_id();
								// time
								DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
								LocalDateTime now = LocalDateTime.now();
								String timestamp = dtf.format(now);
								// count line
								String stagin_load_count = "";
								try {
									stagin_load_count = countLines(file, extention) + "";
								} catch (InvalidFormatException
										| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
									e.printStackTrace();
								}
								//
								String target_dir;

								if (dp.writeDataToBD(columnList, target_tb, values)) {
									file_status = "SU";
									dp.getCdb().insertLog(table, file_status, config_id, timestamp, stagin_load_count,
											file_name);
									target_dir = config.getSuccess_dir();
									if (moveFile(target_dir, file))
										;

								} else {
									file_status = "ERR";
									dp.getCdb().insertLog(table, file_status, config_id, timestamp, stagin_load_count,
											file_name);
									target_dir = config.getErr_dir();
									if (moveFile(target_dir, file))
										;

								}
							}
						}
					}
				}

			}

		} else {
			System.out.println("Path not exists!!!");
			return;
		}
	}

	private boolean moveFile(String target_dir, File file) {
		try {
			BufferedInputStream bReader = new BufferedInputStream(new FileInputStream(file));
			BufferedOutputStream bWriter = new BufferedOutputStream(
					new FileOutputStream(target_dir + File.separator + file.getName()));
			byte[] buff = new byte[1024 * 10];
			int data = 0;
			while ((data = bReader.read(buff)) != -1) {
				bWriter.write(buff, 0, data);
			}
			bReader.close();
			bWriter.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			file.delete();
		}
	}

	private int countLines(File file, String extention)
			throws InvalidFormatException, org.apache.poi.openxml4j.exceptions.InvalidFormatException {
		int result = 0;
		XSSFWorkbook workBooks = null;
		try {
			if (extention.indexOf(".txt") != -1) {
				BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
				String line;
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
					e.printStackTrace();
				}
			}
		}
		return result;
	}
}
