package indi.shui4.disruptor.quickstart;

import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

/**
 * @author shui4
 * @date 2022/4/13
 * @since 0.0.1
 */
public class OrderEventProducer {

  private RingBuffer<OrderEvent> ringBuffer;

  public OrderEventProducer(RingBuffer<OrderEvent> ringBuffer) {
    this.ringBuffer = ringBuffer;
  }

  public void sendData(ByteBuffer bb) {
    long sequence = ringBuffer.next();
    try {
      // 获取下一个可用的序号
      // 根据sequence找到OrderEvent元素。此时获取的OrderEvent是一个没有被填充的对象
      OrderEvent event = ringBuffer.get(sequence);
      event.setValue(bb.getLong(0));
    } finally {
      // 提交发布操作
      ringBuffer.publish(sequence);
    }
  }
}
