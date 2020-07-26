package ETL;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import model.ReadFile;
import util.ConfigUtils;

public class LoadFromLocalToStaging {
	private String configName;

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public void ExtractToDatabase(ReadFile rf, String file_arg) throws ClassNotFoundException, SQLException {
		Config config = ConfigUtils.getConfig(this.configName);
		String target_tb = config.getTarget_tb();
		String file_type = config.getFile_type();
//		String import_dir = config.getImport_dir();
//		String file_Mask = config.getFile_Mask();
		String success_dir = config.getSuccess_dir();
		String delimeter = config.getDelimeter();
		String columnList = config.getColumnList();
		String dataTypes = config.getDataTypes();

		if (!rf.getCdb().tableExist(target_tb)) {
			rf.getCdb().createTable(target_tb, dataTypes, columnList);
		}
		File suc_dir = new File(success_dir);
		if (suc_dir.exists()) {
			String extention = "";
			List<Log> listlog = LogUtils.getConfigByState("ER");
			StringTokenizer str = new StringTokenizer(columnList,",");
			File file = new File(suc_dir.getAbsolutePath()+ File.separator + file_arg);
			if(!file.exists()) {
				System.out.println("file" + file.getName() + "not exists in success directory");
				return;
			}
					// String file_name = file.getName().replaceAll("."+ file_type, "");
				for (Log log :listlog) {
					String file_name = file.getName();
					if (file_name.equals(log.getFile_name())) {
						if (file_type != null) {
							String values = "";
							if (file_type.equals("txt")) {
								values = rf.readValuesTXT(file, str.countTokens());
								extention = ".txt";
							}
							if (file_type.equals("csv")) {
									values = rf.readValuesTXT(file, str.countTokens());
									extention = ".csv";
							} else if (file_type.equals("xlsx")) {
									values = rf.readValuesXLSX(file, str.countTokens());
									extention = ".xlsx";
							}
								if (values != null) {
									// String table = "log";
									String state;
									
									int config_id = config.getConfig_id();

//								time
									Long millis = System.currentTimeMillis();
									Timestamp currentTS = new Timestamp(millis);

//								count line
									String staging_count = "";
									try {
										staging_count = countLines(file, extention) + "";
									} catch (InvalidFormatException
											| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
										e.printStackTrace();
									}

									String target_dir;
									System.out.println("Extracting........");
									if (rf.writeDataToBD(columnList, target_tb, values)) {
										System.out.println("Extract success");
										state = "EXS";
										LogUtils.updateStateForAFile(config_id, state, currentTS,
												Integer.parseInt(staging_count), file_name);
//									if (moveFile(target_dir, file))
//										;
									} else {
										state = "EXF";
										System.out.println("Extract Fail");
										LogUtils.updateStateForAFile(config_id, state, currentTS,
												Integer.parseInt(staging_count), file_name);
										target_dir = config.getErr_dir();
//									if (moveFile(target_dir, file))
//										;
									}
								}
							}
						}

					}
				}
			
		 else {
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
			// file.delete();
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
	public void reExtract() throws ClassNotFoundException, SQLException {
		List<Log> listLog = LogUtils.getConfigByState("EXF");
		for (Log log : listLog) {
			ReadFile rf = new ReadFile();
			this.ExtractToDatabase(rf, log.getFile_name());
		}
	}
	public void reExtract(String config_name) throws ClassNotFoundException, SQLException {
		List<Log> listLog = LogUtils.getConfigByState("EXF");
		for (Log log : listLog) {
			if(log.getConfig_id() == ConfigUtils.getConfig(config_name).getConfig_id()) {
			ReadFile rf = new ReadFile();
			this.ExtractToDatabase(rf, log.getFile_name());
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		ls.setConfigName("f_dangky");
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		ls.ExtractToDatabase(rf , "abc");
	}
}
