package site.ycsb.workloads;

import site.ycsb.*;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Dummy Java Doc.
 */
public class MemcachierWorkload extends Workload {
  private enum MemcachierType {
    GET,
    SET,
    DELETE,
    ADD,
    INCREMENT,
    STATS,
    OTHER,
  }

  private static class MemcachierTransaction {
    private long time;
    private MemcachierType type;
    private int valueSize;
    private String key;
  }

  private long startTime;

  private FileInputStream inputStream;
  private Scanner scanner;

  private float acceleration;
  private float inflation;
  private boolean withDeletion;

  @Override
  public void init(Properties p) throws WorkloadException {
    try {
      inputStream = new FileInputStream(p.getProperty("trace"));
      scanner = new Scanner(inputStream, "UTF-8");
    } catch (Exception e) {
      System.err.println("Failed to open the trace");
      e.printStackTrace();
    }
    acceleration = Float.parseFloat(p.getProperty("acceleration", "1"));
    inflation = Float.parseFloat(p.getProperty("inflation", "1"));
    withDeletion = Boolean.parseBoolean(p.getProperty("withdeletion", "false"));

    startTime = System.nanoTime();
  }

  private synchronized String getLine() {
    String line = null;
    if (scanner.hasNextLine()) {
      line = scanner.nextLine();
    }
    return line;
  }

  private MemcachierTransaction parseTransaction(String line) {
    String[] params = line.split(",");

    MemcachierTransaction transaction = new MemcachierTransaction();
    transaction.time = (acceleration > 0) ? (long) (1000000000 * (Float.parseFloat(params[0]) / acceleration)) : 0;
    transaction.valueSize = (int) Math.min(Long.parseLong(params[4]), Integer.MAX_VALUE);
    transaction.valueSize = (int) (transaction.valueSize * inflation);
    transaction.key = params[1] + "," + params[5];
    /* only consider GET, SET, DELETE */
    switch (Integer.parseInt(params[2])) {
    case 1:
      transaction.type = MemcachierType.GET;
      break;
    case 2:
      transaction.type = MemcachierType.SET;
      if (transaction.valueSize < 0) {
        transaction = null;
      }
      break;
    case 3:
      transaction.type = MemcachierType.DELETE;
      break;
    default:
      transaction = null;
      break;
    }

    return transaction;
  }

  private void executeTransaction(DB db, MemcachierTransaction transaction) {
    byte[] value;
    String valueString;

    /* only consider GET, SET, DELETE */
    switch (transaction.type) {
    case GET:
      db.cacheGet(transaction.key);
      break;
    case SET:
      value = ByteBuffer.allocate(transaction.valueSize).array();
      for (int i = 0; i < transaction.valueSize; ++i) {
        value[i] = (byte)((int)(ThreadLocalRandom.current().nextFloat() * 91) + ' ');
      }
      valueString = new String(value);
      db.cacheSet(transaction.key, valueString);
      break;
    case DELETE:
      if (withDeletion) {
        db.cacheDelete(transaction.key);
      }
      break;
    default:
      break;
    }
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    String line = getLine();
    if (line == null) {
      return false;
    }
    MemcachierTransaction transaction = parseTransaction(line);
    if (transaction != null) {
      executeTransaction(db, transaction);
    }
    return true;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    try {
      String line = getLine();
      if (line == null) {
        return false;
      }
      MemcachierTransaction transaction = parseTransaction(line);
      if (transaction != null) {
        long curTime = System.nanoTime() - startTime;
        if (curTime < transaction.time) {
          long deltaTime = transaction.time - curTime;
          Thread.sleep(deltaTime / 1000000, (int) (deltaTime % 1000000));
        }

        executeTransaction(db, transaction);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }
}


