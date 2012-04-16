package com.yahoo.ycsb.generator;
import java.util.concurrent.*;
public class ZetaCalculator {

    public static final long ITEM_COUNT=10000000000L;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ExecutorService es = Executors.newCachedThreadPool();
        es.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(".999 - " + Double.toString(ZipfianGenerator.zetastatic(ITEM_COUNT, .999)));
            }
        });
        es.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(".9999 - " + Double.toString(ZipfianGenerator.zetastatic(ITEM_COUNT, .9999)));
            }
        });
        es.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(".99999 - " + Double.toString(ZipfianGenerator.zetastatic(ITEM_COUNT, .99999)));
            }
        });
        es.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println(".999999 - " + Double.toString(ZipfianGenerator.zetastatic(ITEM_COUNT, .999999)));
            }
        });
        es.shutdown();
        es.awaitTermination(356, TimeUnit.DAYS);
    }

}
