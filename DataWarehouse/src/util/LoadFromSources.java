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
			System.loadLibrary("chilkat"); //copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public void DownLoad(Config config) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Download");
		String hostname = config.getSrc_url();
		int port = 2227;
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(config.getSrc_user(), config.getSrc_pass());
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch(config.getFile_Mask());//down tat ca cac file bat dau bang sinhvien
		String remotePath = config.getSrc_path(); // /volume1/ECEP/song.nguyen/DW_2020/data
		String localPath = config.getImport_dir(); //thu muc muon down file ve
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			ssh.Disconnect();
			return;
		}
		else {
			System.out.println("DownloadSuccess");
			ssh.Disconnect();
			List<File> listFile = listFile(config.getImport_dir());
			try {
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
	public  List<File> listFile(String dir) {
		System.out.println(dir);
		 File directoryPath = new File(dir);
		 List<File> listFile = new ArrayList<File>();
		 String[] paths = directoryPath.list();
		 for (int i = 0; i < paths.length; i++) {
			listFile.add(new File(dir + File.separator + paths[i]));
		 }
		 return listFile;
	}
	public void checkFile(int config_id,List<File> listFile, String errDir, String sucDir, String checkSum) throws IOException, 
		ClassNotFoundException, ParseException, SQLException {
		for (int i = 0; i < listFile.size(); i++) {
			File f = listFile.get(i);
			if(checkSumCompare(f, checkSum)) {
				copyFileUsingStream(f.getAbsolutePath(), new File(sucDir + File.separator +f.getName()));
				addDownloadLog(config_id,f.getName(), "ER");
				f.delete();
				
			}
			else {
				copyFileUsingStream(f.getAbsolutePath(), new File(sucDir + File.separator +f.getName()));
				addDownloadLog(config_id,f.getName(), "F");
				mailUtils mail = new mailUtils();
				mail.SendMail("", "Download File fail", "Downloading file: "+ f.getName() + "process has been fail");
				f.delete();
			}
		}
	}
	private boolean checkSumCompare(java.io.File file, String checkSum) {
		// TODO Auto-generated method stub
		return true;
	}
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
		Timestamp download_timestamp = new Timestamp(System.currentTimeMillis()) ;
		Timestamp nonValueDate = new Timestamp(2070,12,31,24,00,00,00);
		LogUtils.insertNewLog(config_id, file_name, state, nonValueDate, download_timestamp,
				nonValueDate, -1, -1);
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Config config = ConfigUtils.getConfig("f.txt");
		LoadFromSources LFS = new LoadFromSources();
		LFS.DownLoad(config);
	}

}
