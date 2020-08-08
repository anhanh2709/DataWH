package Launcher;

import java.sql.SQLException;
import java.util.Scanner;

import ETL.LoadFromLocalToStaging;
import ETL.LoadFromSources;
import ETL.LoadFromStagingToWareHouse;
import configuration.Config;
import dao.ControlDatabase;
import model.ReadFile;
import util.ConfigUtils;

public class Launcher {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		boolean run = true;
		while (run) {
			System.out.println("nhap lenh");
			Scanner scanner = new Scanner(System.in);
			String s = scanner.next();
			String[] com = s.split("-");
			String cmd = com[0];
			switch (cmd) {
			case "download":
				LoadFromSources lfs = new LoadFromSources();
				String cfgName = com[1];
				Config config = ConfigUtils.getConfig(cfgName);
				lfs.DownLoad(config);
				break;
			case "extract":
				LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
				String cfgName2 = com[1];
				ls.setConfigName(cfgName2);
				ReadFile rf = new ReadFile();
				ControlDatabase cdb = new ControlDatabase();
				cdb.setConfig_db_name("control");
				cdb.setTarget_db_name("staging");
				cdb.setTable_name("config");
				rf.setCdb(cdb);
				ls.ExtractToDatabase(rf, com[2]);
				break;
			case "transform":
				LoadFromStagingToWareHouse load = new LoadFromStagingToWareHouse();
				String cfgName3 = com[1];
				Config config2 = ConfigUtils.getConfig(cfgName3);
				load.load(config2);
				break;
			case "end":
				run = false;
				break;
			default:
				System.out.println("Lenh khong ho tro");
			}
		}
	}
}
