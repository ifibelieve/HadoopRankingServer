package my.hadoop.test;

import java.io.IOException;

public class Rank {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: rank <in> <out1> <out2> <reducers>");
            System.exit(2);
        }
        
        try {
            SumPoint.runJob(args[0], args[1]);
            Sort.runJob(args[1], args[2], Integer.parseInt(args[3]));
        } catch (Exception e) {
            System.err.println("[Rank runJob]" + e.getMessage());
            e.printStackTrace();
        }
    }
}
