package webcrawler;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ydevaraju on 2/3/19.
 */
public class Consumer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private BlockingQueue<String> queue;
  private int threadId=0;
  private String searchWord;
  private ConcurrentHashMap<String, Integer> resultMap;

  public Consumer(BlockingQueue<String> queue, String searchWord,
      int threadId, ConcurrentHashMap<String, Integer> resultMap) {
    this.queue = queue;
    this.searchWord = searchWord;
    this.threadId = threadId;
    this.resultMap = resultMap;
  }

  @Override
  public void run() {
    boolean running = true;
    String url = null;
    while(running) {
      try
      {
        url = queue.take();
        Connection connection = Jsoup.connect("https://" + url);
        Document htmlDocument = connection.get();

        LOG.info(threadId + " -> Received web page at " + url);
        if(searchForWord(searchWord, htmlDocument)) {
          LOG.info("Search word found in " + url);
          resultMap.put(url,1);
        }
      } catch (SocketTimeoutException ste) {
        LOG.error("Socket timeout exception! " + url);
      } catch(IOException ioe) {
        LOG.info("IOException for URL " + url);
      } catch (InterruptedException e) {
        LOG.info("InterruptedException for URL " + url, e);
      } finally {
        if(queue.size() == 0) {
          running = false;
        }
      }
    }
    LOG.info(threadId + " -> Shutdown!");
  }

  private boolean searchForWord(String searchWord, Document htmlDocument)
  {
    String bodyText = htmlDocument.body().text();
    return bodyText.toLowerCase().contains(searchWord.toLowerCase());
  }
}
