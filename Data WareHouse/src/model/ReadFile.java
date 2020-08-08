package model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.ControlDatabase;

public class ReadFile {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static final String DATE_FORMAT = "yyyy-MM-dd";
	private ControlDatabase cdb;
	private String config_db_name;
	private String target_db_name;
	private String table_name;

	public ReadFile() {
		cdb = new ControlDatabase(this.config_db_name, this.table_name, this.target_db_name);
	}

//	Đọc từng dòng dữ liệu
	private String readLines(String value, String delim) {
		String values = "";

//		tạo ra một lớp StringTokenizer dựa trên chuỗi chỉ định và dấu phân cách.
		StringTokenizer stoken = new StringTokenizer(value, delim);
//		countTokens: Trả về tổng số lượng của các stoken.
		int countToken = stoken.countTokens();
		String lines = "(";

		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
//			nếu là cuối cùng thì ) + | không thì là dấu ,
			lines += (j == countToken - 1) ? "'" + token.trim() + "')|" : "'" + token.trim() + "',";
			values += lines;
			lines = "";
		}
		return values;
	}

//	đọc txt
	public String readValuesTXT(File s_file, int count_field) {
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim = "|";
		try {
//			Đọc một dòng dữ liệu có trong file:
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8")); //mở file để đọc
			String line = bReader.readLine();
//			nếu delim là "\t" thì trả về \t
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
//			Kiểm tra có header hay không
//			nếu là số thì đọc còn k phải số thì bỏ qua
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) {
				values += readLines(line + delim, delim);
			}
//			chạy dòng while ghi hết dữ liệu từng dòng đọc được lại vào values
			while ((line = bReader.readLine()) != null) {
				values += readLines(line + " " + delim, delim);
			}
			bReader.close();
//			trả giá trị từ đầu tới cuối
			return values.substring(0, values.length() - 1);
		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

//	đọc xlsx
	public String readValuesXLSX(File s_file, int countField) {
		String values = ""; //dữ liệu cuối cùng có được
		String value = ""; //dữ liệu đọc từng dòng
		String delim = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file); //file truyền vào
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn); // file xlsx
			XSSFSheet sheet = workBook.getSheetAt(0); //1 sheet trong file xlsx
			Iterator<Row> rows = sheet.iterator(); // lấy từng hàng trong 1 sheet
//			Kiểm tra xem có phần header hay không, nếu không có phần header
//			Gọi rows.next, nếu có header thì vị trí dòng dữ liệu là 1.
//			Nếu kiểm tra mà không có header thì phải set lại cái row bắt đầu
//			ở vị trí 0, hổng ấy là bị sót dữ liệu dòng 1 nha.
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				// iterator lấy danh sách
				rows = sheet.iterator();
			}
			while (rows.hasNext()) { // co lay phan tu tiep theo khong
				Row row = rows.next(); // lay phan tu tiep theo
//				 Kiểm tra coi cái số trường ở trong file excel có đúng với
//				 số trường có trong cái bảng mình tạo sẵn ở trong table
//				 staging không
				if (row.getLastCellNum() < countField + 1 || row.getLastCellNum() > countField + 2) {
					workBook.close();
					return null;
				}

//				Bắt đầu lấy giá trị trong các ô ra:
//				Iterator<Cell> cells = row.cellIterator();
				for (int cn = 0; cn < countField; cn++) {
//					tạo ô trống
					Cell cell = row.getCell(cn, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
//					ép kiểu cho ô trống
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim;
						} else {
							value += (long) cell.getNumericCellValue() + delim;
						}
						break;
					case STRING:
						value += cell.getStringCellValue() + delim;
						break;
//						công thức trong excel
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim;
							break;
						default:
							value += " " + delim;
							break;
						}
						break;
					case BLANK:
//						giá trị trống
						value += " " + delim;
						break;
					default:
//						dòng bị trống thì bỏ qua
						if (cn < 2) {
							value += (long) cell.getNumericCellValue() + delim;
						} else
//							giá trị trống
							value += " " + delim;
						break;
					}
				}
//				nếu đọc đủ số trường rồi thì hàng dữ liệu đó + với dấu |
				if (row.getLastCellNum() == countField + 1) {
					value += "|";
				}
				values += readLines(value, delim);
				value = "";
			}
//			đọc xong thì đóng lại
			workBook.close();
			fileIn.close();
//			trả về dữ liệu từ đầu đến cuối
			return values.substring(0, values.length() - 1);
		} catch (Exception e) {
			return null;
		}
	}

	public boolean writeDataToBD(String column_list, String target_table, String values) throws ClassNotFoundException {
		if (cdb.insertValues(column_list, values, target_table))
			return true;
		return false;
	}

	public void setConfig_db_name(String config_db_name) {
		this.config_db_name = config_db_name;
	}

	public void setTarget_db_name(String target_db_name) {
		this.target_db_name = target_db_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public ControlDatabase getCdb() {
		return cdb;
	}

	public void setCdb(ControlDatabase cdb) {
		this.cdb = cdb;
	}

}
