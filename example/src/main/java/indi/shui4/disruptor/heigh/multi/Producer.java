package indi.shui4.disruptor.heigh.multi;

import com.lmax.disruptor.RingBuffer;

/**
 * @author shui4
 * @date 2022/4/14
 * @since 0.0.1
 */
public class Producer {
  private RingBuffer<Order> ringBuffer;

  public Producer(RingBuffer<Order> ringBuffer) {
    this.ringBuffer = ringBuffer;
  }

  public void sendData(String uuid) {
    long sequence = ringBuffer.next();
    try {
      Order event = ringBuffer.get(sequence);
      event.setId(uuid);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 发布指定的序列。此操作将此特定消息标记为可供阅读
      ringBuffer.publish(sequence);
    }
  }
}
