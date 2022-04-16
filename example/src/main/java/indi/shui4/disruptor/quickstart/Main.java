package indi.shui4.disruptor.quickstart;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author shui4
 * @date 2022/4/13
 * @since 0.0.1
 */
@SuppressWarnings("AlibabaThreadPoolCreation")
public class Main {
  public static void main(String[] args) {
    // 建议使用自定义线程池
    ExecutorService executor =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    // 1.实例化disruptor对象
    Disruptor<OrderEvent> disruptor = newDisruptor(executor);
    RingBuffer<OrderEvent> ringBuffer = disruptor.start();
    //    RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();
    OrderEventProducer producer = new OrderEventProducer(ringBuffer);
    ByteBuffer bb = ByteBuffer.allocate(8);
    for (int i = 0; i < 100; i++) {
      bb.putLong(0, i);
      producer.sendData(bb);
    }
    disruptor.shutdown();
    executor.shutdown();
  }

  private static Disruptor<OrderEvent> newDisruptor(ExecutorService executor) {
    Disruptor<OrderEvent> disruptor =
        new Disruptor<>(
            new OrderEventFactory(),
            1024 * 1024,
            executor,
            ProducerType.SINGLE,
            new BlockingWaitStrategy());
    // 监听事件监听器
    disruptor.handleEventsWith(new OrderEventHandler());
    return disruptor;
  }
}
