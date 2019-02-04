package webcrawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.w3c.dom.DOMConfiguration;
import org.slf4j.LoggerFactory;

/**
 * Created by ydevaraju on 2/3/19.
 */
public class Crawler {

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  final BlockingQueue<String> queueOfUrls = new LinkedBlockingQueue<>(20);

  private void startProducer() {
    new Thread(new Producer(queueOfUrls)).start();
  }

  private void startConsumer(String searchWord) throws IOException, InterruptedException {
    ConcurrentHashMap<String, Integer> results = new ConcurrentHashMap<>();
    Thread[] threads = new Thread[20];
    for(int i=1; i<=20; i++) {
      threads[i-1]= new Thread(new Consumer(queueOfUrls, searchWord, i, results));
      threads[i-1].start();
    }
    for(int i=1; i<=20; i++) {
      threads[i-1].join();
    }
    BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
    for (String keys : results.keySet()) {
      writer.write(keys + "\n");
    }
    writer.close();
  }

  public static void main(String args[]) throws IOException, InterruptedException {
    LOG.info("Starting application.");
    final BlockingQueue<String> queueOfUrls = new LinkedBlockingQueue<>(20);
    Crawler crawler = new Crawler();
    crawler.startProducer();
    // Pass search word
    crawler.startConsumer("Password");
    LOG.info("Exiting application.");
  }

}
