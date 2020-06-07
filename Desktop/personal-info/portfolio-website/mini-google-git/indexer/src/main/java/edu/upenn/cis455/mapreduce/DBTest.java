package edu.upenn.cis455.mapreduce;

import java.io.File;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class DBTest {

	public static void main(String[] args) {
		
		String js = "{key: extra,value: 3}";
		
	}
		
	/**
	 * delete the whole directory
	 * @param dirPath
	 */
	public static void deleteDir(String dirPath) {
		File file = new File(dirPath);
		if (file.exists()) {
			if (file.isFile()) {
				file.delete();
			} else {
				File[] files = file.listFiles();
				if (files == null) {
					file.delete();
				} else {
					for (int i = 0; i < files.length; i++) {
						deleteDir(files[i].getAbsolutePath());
					}
					file.delete();
				}
			}
		}
	}
}
