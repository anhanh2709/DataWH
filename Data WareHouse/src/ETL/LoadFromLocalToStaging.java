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
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import util.LogUtils;

import configuration.Config;
import configuration.Log;
import dao.ControlDatabase;
import mail.mailUtils;
import modal.ReadFile;

public class LoadFromLocalToStaging {

	static final String EXT_TEXT = ".txt";
	static final String EXT_CSV = ".csv";
	static final String EXT_EXCEL = ".xlsx";

	private String configName;

	public String getConfigName() {
		return configName;
	}

	public void setConfigName(String configName) {
		this.configName = configName;
	}

	public void ExtractToDatabase(ReadFile rf) throws ClassNotFoundException, SQLException {
		List<Config> listconfig = rf.getCdb().loadAllConfig(this.configName);

		//lay cac truog trog dong config ra
		for (Config config : listconfig) {
			String extention = "";
			String target_tb = config.getTarget_tb();
			String file_type = config.getFile_type();
	//		String import_dir = config.getImport_dir();
	//		String file_Mask = config.getFile_Mask();
			String success_dir = config.getSuccess_dir();
			String delimeter = config.getDelimeter();
			String columnList = config.getColumnList();
			String dataTypes = config.getDataTypes();
			
			System.out.println(file_type);
			System.out.println(dataTypes);
			System.out.println(target_tb);
			System.out.println(success_dir);
			
			//tao table
			if (!rf.getCdb().tableExist(target_tb)) {
				System.out.println(dataTypes);
				rf.getCdb().createTable(target_tb, dataTypes, columnList);
			}

//			Lấy state=ER
			Log log = LogUtils.getConfigByState("ER");		
			
//			lấy filename trong log
			String file_name = log.getFile_name();
			System.out.println(file_name);
			String sourceFile = success_dir + File.separator + file_name;
			System.out.println(sourceFile);
			StringTokenizer str = new StringTokenizer(columnList,delimeter);
			File file = new File(sourceFile);
		
			//lay duoi file xem file do la file gi
			extention = file.getPath().endsWith(".xlsx") ? EXT_EXCEL
					: file.getPath().endsWith(".txt") ? EXT_TEXT : EXT_CSV;
			
//			neu file ton tai
			if (file.exists()) {
				String values = "";
				// Nếu file là .txt thì đọc file .txt
				if (extention.equals(".txt")) {
					values = rf.readValuesTXT(file, str.countTokens());
					extention = ".txt";
					// Nếu file là .xlsx thì đọc file .xlsx
				} else if (extention.equals(".xlsx")) {
					values = rf.readValuesXLSX(file, str.countTokens());
					extention = ".xlsx";
				}
				System.out.println(values);
				
				// Nếu đọc được giá trị rồi
				
				if (values != null) {

					String state;
					int config_id = config.getConfig_id();
					
					
					Long millis = System.currentTimeMillis();
					Timestamp currentTS = new Timestamp(millis);

					
//					count line
					String staging_count = "";
					try {
						staging_count = countLines(file, extention) + "";
					} catch (InvalidFormatException
							| org.apache.poi.openxml4j.exceptions.InvalidFormatException e) {
						e.printStackTrace();
					}

//					String target_dir;

					if (rf.writeDataToBD(columnList, target_tb, values)) {
						System.out.println("trueeee");
						state = "EXS";
						System.out.println("Config_id = " +config_id + " TS = " +currentTS.toString() + "count =" +staging_count );
						LogUtils.updateNewState(config_id, state, currentTS, Integer.parseInt(staging_count));
//						target_dir = config.getSuccess_dir();
//						if (moveFile(target_dir, file))
//							;
						} else {
						state = "EXF";
						LogUtils.updateNewState(config_id, state, currentTS, Integer.parseInt(staging_count));
					
						mailUtils mail = new mailUtils();
						mail.SendMail("", "Load File fail", "Load file: "+ sourceFile + "process has been fail");
//						target_dir = config.getErr_dir();
//						if (moveFile(target_dir, file))
//							;
					}
	
				}
		} 
//			file khong ton tai
			else {
			System.out.println("Path not exists!!!");
			return;
		}

	}

}



//	private boolean moveFile(String target_dir, File file) {
//		try {
//			BufferedInputStream bReader = new BufferedInputStream(new FileInputStream(file));
//			BufferedOutputStream bWriter = new BufferedOutputStream(
//					new FileOutputStream(target_dir + File.separator + file.getName()));
//			byte[] buff = new byte[1024 * 10];
//			int data = 0;
//			while ((data = bReader.read(buff)) != -1) {
//				bWriter.write(buff, 0, data);
//			}
//			bReader.close();
//			bWriter.close();
//			return true;
//		} catch (IOException e) {
//			e.printStackTrace();
//			return false;
//		} finally {
//			// file.delete();
//		}
//	}

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

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		ls.setConfigName("f_monhoc");
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		ls.ExtractToDatabase(rf);
	}
}
