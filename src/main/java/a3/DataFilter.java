package a3;

import java.io.File;
import java.io.PrintWriter;
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
		System.out.println(new File(path1).getCanonicalPath());
		Scanner sc = new Scanner(new File(path1));
		TreeMap<Double, String> movies = new TreeMap<Double, String>();
		TreeMap<Integer, String> users = new TreeMap<Integer, String>();
		
		while(sc.hasNext()){
			String mID = sc.next();
			Double rating = sc.nextDouble();
			movies.put(rating, mID);
		}
		String resultMovies[] = new String[10];
		int i = 0;
		for(String s: movies.descendingMap().values()){
			resultMovies[i] = s;
			i++;
			if(i > 9)
				break;
		}
		sc.close();
		sc = new Scanner(new File(path2));
		while(sc.hasNext()){
			String mID = sc.next();
			Double rating = sc.nextDouble();
			movies.put(rating, mID);
		}
		String resultUsers[] = new String[10];
		i = 0;
		for(String s: movies.descendingMap().values()){
			resultUsers[i] = s;
			i++;
			if(i > 9)
				break;
		}
		
	    PrintWriter pr = new PrintWriter("Results");    

	    pr.println("Movies");
	    for (i=0; i< 10 ; i++)
	    {
	        pr.println(resultMovies[i]);
	    }
	    pr.println("\nUsers");
	    for (i=0; i< 10 ; i++)
	    {
	        pr.println(resultUsers[i]);
	    }
	    pr.close();
		//System.exit(exitCode1);
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
