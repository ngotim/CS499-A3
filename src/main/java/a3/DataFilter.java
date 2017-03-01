package a3;

import java.io.File;
import java.util.Comparator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.util.ToolRunner;

public class DataFilter {

	public static void main(String[] args) throws Exception {
		System.out.println(args[0] + " " + args[1] + " " + args[2]);
		int exitCode = ToolRunner.run(new MovieDriver(), args);
		int exitCode1 = ToolRunner.run(new UserDriver(), args);
		String path1 = "output1/part-r-00000";
		String path2 = "output2/part-r-00000";
		Scanner sc = new Scanner(new File(path1));
		TreeMap<String, Double> movies = new TreeMap<String, Double>();
		TreeMap<String, Integer> users = new TreeMap<String, Integer>();
		
		while(sc.hasNextLine()){
			String mID = sc.next();
			Double rating = sc.nextDouble();
			
			
		}
		System.exit(exitCode1);
	}

	public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
	    Comparator<K> valueComparator =  new Comparator<K>() {
	        public int compare(K k1, K k2) {
	            int compare = map.get(k2).compareTo(map.get(k1));
	            if (compare == 0) return 1;
	            else return compare;
	        }
	    };
	    Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
	    sortedByValues.putAll(map);
	    return sortedByValues;
	}
}
