package indi.shui4.disruptor.quickstart;

import com.lmax.disruptor.EventHandler;

/**
 * @author shui4
 * @date 2022/4/13
 * @since 0.0.1
 */
public class OrderEventHandler implements EventHandler<OrderEvent> {
  @Override
  public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) throws Exception {
    System.out.println("消费者： " + event.getValue());
  }
}
