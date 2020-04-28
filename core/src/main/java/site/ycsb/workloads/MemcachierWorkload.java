package site.ycsb.workloads;

import site.ycsb.*;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.*;

/**
 * Dummy Java Doc.
 */
public class MemcachierWorkload extends Workload implements Runnable {
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
    private float time;
    private MemcachierType type;
    private int keySize;
    private int valueSize;
    private long keyID;
  }

  private long startTime;
  private LinkedBlockingQueue<MemcachierTransaction> transactionList;

  private FileInputStream inputStream;
  private Scanner scanner;

  private final long prefetchSize = 4194304;
  private final long needPrefetchSize = prefetchSize / 2;
  private Thread prefetchThread;

  @Override
  public void init(Properties p) throws WorkloadException {
    startTime = System.nanoTime();
    transactionList = new LinkedBlockingQueue<>();
    try {
      inputStream = new FileInputStream(p.getProperty("trace"));
      scanner = new Scanner(inputStream, "UTF-8");
    } catch (Exception e) {
      System.err.println("Failed to open the trace");
      e.printStackTrace();
    }
    prefetchThread = new Thread(this);
    prefetchThread.start();
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    /* not implemented */
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    try {
      MemcachierTransaction transaction = transactionList.poll(10, TimeUnit.SECONDS);
      if (transaction == null) {
        return false;
      }

      float curTime = ((float)(System.nanoTime() - startTime)) / 1000000000;
      if (curTime < transaction.time) {
        Thread.sleep((int)((transaction.time - curTime) * 1000));
      }

      /* does not support arbitrary byte array as key */
      String keyString = String.valueOf(transaction.keyID);

      byte[] value;
      String valueString;

      switch (transaction.type) {
      case GET:
        db.cacheGet(keyString);
        break;
      case SET:
      case ADD:
      case INCREMENT:
        value = ByteBuffer.allocate(transaction.valueSize).array();
        valueString = new String(value);
        db.cacheSet(keyString, valueString);
        break;
      case DELETE:
        db.cacheDelete(keyString);
        break;
      case STATS:
        db.cacheStat();
        break;
      default:
        break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return true;
  }

  @Override
  public void run() {
    try {
      while (true) {
        if (transactionList.size() <= needPrefetchSize) {
          for (int i = 0; i < prefetchSize; ++i) {
            if (scanner.hasNextLine()) {
              String line = scanner.nextLine();
              String[] params = line.split(",");

              MemcachierTransaction transaction = new MemcachierTransaction();
              transaction.time = Float.parseFloat(params[0]);
              transaction.keySize = (int)Math.min(Long.parseLong(params[3]), Integer.MAX_VALUE);
              transaction.valueSize = (int)Math.min(Long.parseLong(params[4]), Integer.MAX_VALUE);
              transaction.keyID = Long.parseLong(params[5]);
              switch (Integer.parseInt(params[2])) {
              case 1:
                transaction.type = MemcachierType.GET;
                break;
              case 2:
                transaction.type = MemcachierType.SET;
                if (transaction.valueSize <= 0) {
                  continue;
                }
                break;
              case 3:
                transaction.type = MemcachierType.DELETE;
                break;
              case 4:
                transaction.type = MemcachierType.ADD;
                if (transaction.valueSize <= 0) {
                  continue;
                }
                break;
              case 5:
                transaction.type = MemcachierType.INCREMENT;
                if (transaction.valueSize <= 0) {
                  continue;
                }
                break;
              case 6:
                transaction.type = MemcachierType.STATS;
                break;
              default:
                transaction.type = MemcachierType.OTHER;
              }
              transactionList.add(transaction);
            } else {
              return;
            }
          }
        }
        if (transactionList.size() > needPrefetchSize) {
          Thread.sleep(500);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}


