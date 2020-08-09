package ETL;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import configuration.Config;
import mail.mailUtils;
import util.ConfigUtils;
import util.DBConnection;
import util.LogUtils;

public class LoadFromSources {
	static {
		try {
			// 2. load thư viện chilkat
			System.load("D:\\DataWareHouse\\chilkat.dll"); //copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			// kiểm tra load thành công hay thất bại
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public void DownLoad(Config config) {
		// 7. sử dụng kết nối ssh
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("DownLoad");
		ssh.UnlockComponent("Download");
		// 5. lấy url là host và kết nối tới url đó
		String hostname = config.getSrc_url();
		int port = 2227;
		boolean success = ssh.Connect(hostname, port);
		// kiểm tra kết nối
		// nếu lỗi thì in thông báo
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			// ngưng thục hiện
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		//6. tự động lấy username và password trong config
		success = ssh.AuthenticatePw(config.getSrc_user(), config.getSrc_pass());
		// kiểm tra kết nối, nếu có lỗi dừng download
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();
		// 7. sử dụng kết nối ssh
		success = scp.UseSsh(ssh);
		// kiểm tra kết nối sau khi kết nối ssh
		// nếu có lỗi thì dừng download
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		// 8. lấy file_mask từ trong config, các file trên host phải khớp với file_mask định sẵn mới được download về
		scp.put_SyncMustMatch(config.getFile_Mask());//down tat ca cac file bat dau bang sinhvien
		// 9. lấy từ config ra remotePath : đường dẫn của các file trên host
		String remotePath = config.getSrc_path(); // /volume1/ECEP/song.nguyen/DW_2020/data
		// 9. lấy từ config ra localPath: đường dẫn trên local mà ta muốn download file về
		String localPath = config.getImport_dir(); //thu muc muon down file ve
		// 10. thực hiện download từ remotePath về localPath
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		// kiểm tra down load 
		if (success != true) {
			// In lỗi (nếu có)
			System.out.println(scp.lastErrorText());
			// đóng kết nối tới server
			ssh.Disconnect();
			// nếu có lỗi thì dừng download
			return;
		}
		// download thành công
		else {
			//In thông báo download thành công
			System.out.println("DownloadSuccess");
			// đóng kết nối tới server
			ssh.Disconnect();
			// 11. Lấy danh sách file down về được
			List<File> listFile = listFile(config.getImport_dir());
			try {
				// kiểm tra các file đã download về và di chuyển đến sucDir hoặc errDir
				checkFile(config.getConfig_id(), listFile, config.getErr_dir(), config.getSuccess_dir(), "???");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	// lấy danh sách các file vừa down về
	public  List<File> listFile(String dir) {
		 File directoryPath = new File(dir);
		 List<File> listFile = new ArrayList<File>();
		 String[] paths = directoryPath.list();
		 for (int i = 0; i < paths.length; i++) {
			listFile.add(new File(dir + File.separator + paths[i]));
		 }
		 return listFile;
	}
	// phương thức kiểm tra file
	public void checkFile(int config_id,List<File> listFile, String errDir, String sucDir, String checkSum) throws IOException, 
		ClassNotFoundException, ParseException, SQLException {
		for (int i = 0; i < listFile.size(); i++) {
			File f = listFile.get(i);
			// kiểm tra file có tồn tại trong log chưa
			// nếu có sẽ không xử lý và xóa file đó đi
			// nếu không có  thì xử lý
			if(!checkExists(f.getName(),config_id)) {
			if(checkSumCompare(f, checkSum)) {
				// 12.1 copy file từ importDir sang sucDir
				copyFileUsingStream(f.getAbsolutePath(), new File(sucDir + File.separator + f.getName()));
				// 12.1.2 ghi log cho file mới được download, đặt trạng thái là ER (extract ready)
				addDownloadLog(config_id,f.getName(), "ER");
				// 12.1.3 gửi mail thông báo download thành công
				mailUtils.SendMail("", "download File success", "Downloading file: "+ f.getName() + "process has been successed");
				// xóa file trong import
				f.delete();
			}
			else {
				// 12.2 copy file từ importDỉ sang errDir
				copyFileUsingStream(f.getAbsolutePath(), new File(errDir + File.separator + f.getName()));
				// 12.2.2 ghi log cho file download không thành công, đặt trạng thái là F(fail)
				addDownloadLog(config_id,f.getName(), "F");
				// 12.2.3 gửi mail thông báo download thất bại
				mailUtils.SendMail("", "Download File fail", "Downloading file: "+ f.getName() + "process has been fail");
				// Xóa file trong import
				f.delete();
			}
			}
			else {
				// Xóa file trong importDir vì file đã tồn tại rồi
				f.delete();
			}
		}
	}
	private boolean checkExists(String name, int config_id) throws SQLException {
		// kiểm tra trong log xem file đã tồn tại hay chưa
		return LogUtils.checkExistFileName(name,config_id);
	}
	private boolean checkSumCompare(java.io.File file, String checkSum) {
		// TODO Auto-generated method stub
		// Kiểm tra file download về có vẹn toàn hay không thông qua checksum
		// hiện tại chưa có hỗ trợ checksum
		return true;
	}
	// phương thức copyfile
	private  void copyFileUsingStream(String source, File dest) throws IOException {
		    FileInputStream is = new FileInputStream(source);
	        FileOutputStream os = new FileOutputStream(dest);
	        byte[] buffer = new byte[1024];
	        int length;
	        while ((length = is.read(buffer)) > 0) {
	            os.write(buffer, 0, length);
	        }
	        is.close();
	        os.close();
	}
	public void addDownloadLog(int config_id,String file_name, String state) throws ParseException, ClassNotFoundException, SQLException {
		// ngày Hien tai => ngày download trong log
		Timestamp download_timestamp = new Timestamp(System.currentTimeMillis()) ;
		// ngày có giá trị giữ chỗ
		// sử dụng khi extract và transform 
		GregorianCalendar cal = new GregorianCalendar(2070,12,31);
		long millis = cal.getTimeInMillis();
		Timestamp nonValueDate = new Timestamp(millis);
		// ghi log trong table log, các giá trị 1 và -1 cũng là giá trị giữ chỗ cho extrat và transform
		LogUtils.insertNewLog(config_id, file_name, state, nonValueDate, download_timestamp,
				nonValueDate, -1, -1);
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// 3. lấy config có tên tương ứng từ database control, table config thông qua lớp configUntil
		Config config = ConfigUtils.getConfig("f_sinhvien");
		LoadFromSources LFS = new LoadFromSources();
		// 4. gọi phương thức download 
		LFS.DownLoad(config);
	}

}
