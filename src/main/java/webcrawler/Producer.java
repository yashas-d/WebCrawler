package webcrawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.validator.UrlValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ydevaraju on 2/3/19.
 */
public class Producer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
  public static final String SOURCE_URL =
      "https://s3.amazonaws.com/fieldlens-public/urls.txt";
  private BlockingQueue<String> queue;
  public Producer(BlockingQueue<String> queue) {
    this.queue = queue;
  }

  @Override
  public void run() {
    try(BufferedReader in = new BufferedReader(
        new InputStreamReader(new URL(SOURCE_URL).openStream()))) {
      String line = null;
      String[] tokens = null;
      int count = 1;
      while((line = in.readLine()) != null) {
        tokens = line.split(",");
        try {
          String url = tokens[1].substring(1, tokens[1].length()-1);
          if(validateUrl(url)) {
            queue.put(url);
            LOG.info(count++ + ". Adding url: " + url);
          } else {
            LOG.info("URL validation failed! " + url);
          }
        } catch (InterruptedException e) {
          LOG.error("InterruptedException at producer.", e);
        }
      }
    } catch (IOException e) {
      LOG.error("IOException at producer.", e);
    }
    LOG.info("Producer is exiting.");
  }

  private boolean validateUrl(String url) {
    String[] schemes = {"http","https"};
    UrlValidator urlValidator = new UrlValidator(schemes);
    return urlValidator.isValid("https://" + url);
  }
}
