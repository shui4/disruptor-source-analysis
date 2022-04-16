package indi.shui4.disruptor.quickstart;

import com.lmax.disruptor.EventFactory;

/**
 * 订单工厂
 *
 * @author shui4
 * @date 2022/4/13
 * @since 0.0.1
 */
public class OrderEventFactory implements EventFactory<OrderEvent> {

  @Override
  public OrderEvent newInstance() {
    return new OrderEvent();
  }
}
