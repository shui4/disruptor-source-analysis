package indi.shui4.disruptor.heigh.multi;

import cn.hutool.log.Log;
import com.lmax.disruptor.WorkHandler;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shui4
 * @date 2022/4/14
 * @since 0.0.1
 */
public class Consumer implements WorkHandler<Order> {

  private static final Log log = Log.get(Consumer.class);
  private static final Random random = new Random();
  private final String consumerId;
  private final AtomicInteger count = new AtomicInteger();

  public Consumer(String consumerId) {
    this.consumerId = consumerId;
  }

  public AtomicInteger getCount() {
    return count;
  }

  @Override
  public void onEvent(Order event) throws Exception {
    Thread.sleep(random.nextInt(5));
    log.info(
        "当前消费者ID:{},事件ID:{}，线程ID:{}", consumerId, event.getId(), Thread.currentThread().getId());
    count.incrementAndGet();
  }
}
