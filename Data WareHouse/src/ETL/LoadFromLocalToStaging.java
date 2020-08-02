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
import mail.mailUtils;
import model.ReadFile;
import util.ConfigUtils;

public class LoadFromLocalToStaging {

	String filetxt = ".txt";
	String filecsv = ".csv";
	String filexlsx = ".xlsx";

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
			StringTokenizer str = new StringTokenizer(columnList, ",");
//			duong dan den file
			File file = new File(suc_dir + File.separator + file_arg);
			System.out.println(file);

//			file khong ton tai
			if (!file.exists()) {
				System.out.println("file" + file.getName() + "not exists in success directory");
				return;
			}

//			lay list file trong log
			for (Log log : listlog) {
//				lay ten file trong dir
				String file_name = file.getName();
//				kiem xem file trong dir có trong log k
				if (file_name.equals(log.getFile_name())) {
//					lay duoi file de xem file do la loai nao
					extention = file.getPath().endsWith(".xlsx") ? filexlsx
							: file.getPath().endsWith(".txt") ? filetxt : filetxt;

					if (extention != null) {
						String values = "";
						if (extention.equals(".txt")) {
							values = rf.readValuesTXT(file, str.countTokens());
							extention = ".txt";
						}
						if (extention.equals(".csv")) {
							values = rf.readValuesTXT(file, str.countTokens());
							extention = ".csv";
						} else if (extention.equals(".xlsx")) {
							values = rf.readValuesXLSX(file, str.countTokens());
							extention = ".xlsx";
						}
						System.out.println(values);

						if (values != null) {
							
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

							System.out.println("Extracting........");

							String target_dir;
//							ghi du lieu doc duoc vao table
							if (rf.writeDataToBD(columnList, target_tb, values)) {
								System.out.println("Extract success");
								state = "EXS";
								LogUtils.updateStateForAFile(config_id, state, currentTS,
										Integer.parseInt(staging_count), file_name);

							} else {
								state = "EXF";
								System.out.println("Extract Fail");
								LogUtils.updateStateForAFile(config_id, state, currentTS,
										Integer.parseInt(staging_count), file_name);
								target_dir = config.getErr_dir();
								if (moveFile(target_dir, file))
									;
//								mailUtils.SendMail("kanh2709@gmail.com", "Load file từ local lên staging",
//										"Load thất bại!!!");
							}
						}
					}
				}

			}
		} else {
			System.out.println("Path not exists!!!");
//			mailUtils.SendMail("kanh2709@gmail.com", "Load file từ local lên staging",
//					"file không tồn tại!!!");
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
		ls.setConfigName("f_lophoc");
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		ls.ExtractToDatabase(rf, "sinhvien_chieu_nhom13.xlsx");
	}
}
