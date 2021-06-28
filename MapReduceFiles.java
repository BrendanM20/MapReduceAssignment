/*
Brendan Mckeown - 18733641
 */

import java.util.*;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

public class MapReduceFiles {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt");

        }

        Map<String, String> input = new HashMap<String, String>();
        try {
            input.put(args[0], readFile(args[0]));
            input.put(args[1], readFile(args[1]));
            input.put(args[2], readFile(args[2]));
        }
        catch (IOException ex)
        {
            System.err.println("Error reading files...\n" + ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }


        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            Scanner sc = new Scanner(System.in); // Scanner to read in number of lines per thread
            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };
            String lines = input.toString(); // puts all the text from the txt files to a string
            /*
            Splits the text of all three text files by the "." This gives an array with all the lines from the
            txt files
             */
            String[] allLines = lines.split("\\.");
            int loopcounter = 0;
            List<Thread> mapCluster = new ArrayList<Thread>(input.size());
            System.out.println("Please enter number of lines per thread");
            int numofLines = sc.nextInt(); // asks the user for the number of configurable lines per thread
            long timebeforeMap = System.currentTimeMillis(); //records time before map
                for (int i = 0; i < allLines.length; i++) { // iterates through all lines of txt files
                    loopcounter++;
                    /*
                    As the for loop iterates the if loop is used when the user defined number i.e "numofLines"
                    divided by the number of times the for loop is called equals zero. This allows to specify
                    the number of lines per thread
                     */
                    if (loopcounter % numofLines == 0) {
                        for (Map.Entry<String, String>reduce : input.entrySet()){
                        final String file = reduce.getKey();
                        final String contents = reduce.getValue();

                        Thread t = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                map(file, contents, mapCallback);
                            }
                        });
                        mapCluster.add(t);
                        t.start();
                    }
                }
           }
            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            long timeAfterMap = System.currentTimeMillis(); //records time after map
            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }
            // REDUCE:
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };
            int counter =0;
            Scanner sc1 = new Scanner(System.in); // other scanner used for number of words per reduce thread
            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
            System.out.println("Please enter number of words per thread");
            int numofWords = sc1.nextInt(); //asks the user for the number of words per reduce thread
            long timeBeforeReduce = System.currentTimeMillis();//records time before reduce
            /*
            iterates through all grouped words
             */
           for (int i =0; i < groupedItems.keySet().size() ; i++) {
               counter++;
               /*
                    As the for loop iterates the if loop is used when the user defined number i.e "numofWords"
                    divided by the number of times the for loop is called equals zero. This allows to specify
                    the number of number of words per thread
                     */
               if (counter % numofWords == 0) {
                   for (Map.Entry<String, List<String>> group : groupedItems.entrySet()) {
                       final String word = group.getKey();
                       final List<String> list = group.getValue();
                       Thread t = new Thread(new Runnable() {
                           @Override
                           public void run() {
                               reduce(word, list, reduceCallback);
                           }
                       });
                       reduceCluster.add(t);
                       t.start();
                   }
               }
           }

            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            long timeAfterReduce = System.currentTimeMillis(); //records time after reduce

            System.out.println(output);
            System.out.println("Time taken to map  ");
            System.out.println(timeAfterMap - timebeforeMap);
            System.out.println("Time taken to reduce  ");
            System.out.println(timeAfterReduce - timeBeforeReduce);
        }
    }

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            word = word.replaceAll("\\p{Punct}", "");//used to get only proper words in the output
            mappedItems.add(new MappedItem(word, file));
        }
    }

    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            word = word.replaceAll("\\p{Punct}", ""); //used to get only proper words in the output
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

}
