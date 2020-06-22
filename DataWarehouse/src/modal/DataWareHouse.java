package modal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.ControlDatabase;

public class DataWareHouse {
	private String config_name;

	public String getConfig_name() {
		return config_name;
	}

	public void setConfig_name(String config_name) {
		this.config_name = config_name;
	}

	public static void main(String[] args) throws ClassNotFoundException {
		DataWareHouse dw = new DataWareHouse();
		dw.setConfig_name("f_txt");
		DataProcess dp = new DataProcess();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("controldb");
		cdb.setTarget_db_name("datawarehouse");
		cdb.setTable_name("configuration1");
		dp.setCdb(cdb);
		dw.ExtractToDB(dp);
	}

	public void ExtractToDB(DataProcess dp) throws ClassNotFoundException {
		String target_table = dp.getCdb().selectField("target_table", this.config_name);
		System.out.println(target_table);
		if (!dp.getCdb().tableExist(target_table)) {
			String variabless = dp.getCdb().selectField("variabless", this.config_name);
			System.out.println(variabless);
			String column_list = dp.getCdb().selectField("column_list", this.config_name);
			dp.getCdb().createTable(target_table, variabless, column_list);
		}
		String file_type = dp.getCdb().selectField("file_type", this.config_name);
		System.out.println(file_type);
		String import_dir = dp.getCdb().selectField("import_dir", this.config_name);
		String delim = dp.getCdb().selectField("delimeter", this.config_name);
		String column_list = dp.getCdb().selectField("column_list", this.config_name);

		File imp_dir = new File(import_dir);
		if (imp_dir.exists()) {
			String extention = "";
			File[] listFile = imp_dir.listFiles();
			for (File file : listFile) {
				if (file.getName().indexOf(file_type) != -1) {
					System.out.println(7);
					String values = "";
					if (file_type.equals(".txt")) {
						values = dp.readValuesTXT(file, delim);
						extention = ".txt";
					} else if (file_type.equals(".xlsx")) {
						values = dp.readValuesXLSX(file);
						extention = ".xlsx";
					}
					if (values != null) {
						String table = "data_file";
						String file_status;
						String config_id = dp.getCdb().selectField("config_id", this.config_name);
						// time
						DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
						LocalDateTime now = LocalDateTime.now();
						String timestamp = dtf.format(now);
						// count line
						String stagin_load_count = "";
						try {
							stagin_load_count = countLines(file, extention) + "";
						} catch (org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
							e.printStackTrace();
						}
						//
						String target_dir;
						String file_name = file.getName().replaceAll(file_type, "");
						if (dp.writeDataToBD(column_list, target_table, values)) {
							file_status = "SU";
							dp.getCdb().insertLog(table, file_status, config_id, timestamp, stagin_load_count,
									file_name);
							target_dir = dp.getCdb().selectField("success_dir", this.config_name);
							if (moveFile(target_dir, file))
								;

						} else {
							file_status = "ERR";
							dp.getCdb().insertLog(table, file_status, config_id, timestamp, stagin_load_count,
									file_name);
							target_dir = dp.getCdb().selectField("error_dir", this.config_name);
							if (moveFile(target_dir, file))
								;

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
