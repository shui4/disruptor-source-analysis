package indi.shui4.disruptor.heigh.multi;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author shui4
 * @date 2022/4/14
 * @since 0.0.1
 */
public class MultiMain {
  private static final int _1KB = 1024 * 1024;

  public static void main(String[] args) throws InterruptedException {
    RingBuffer<Order> ringBuffer =
        RingBuffer.create(
            // 多生产者
            ProducerType.MULTI, Order::new, _1KB, new YieldingWaitStrategy());
    // 创建一个屏障
    SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
    // 构建多消费者
    Consumer[] consumerArray = createConsumers();
    WorkerPool<Order> workerPool =
        new WorkerPool<>(
            ringBuffer,
            sequenceBarrier,
            // 异常处理类
            new EventExceptionHandler(),
            // 绑定多消费者
            consumerArray);
    // 设置多个消费者的sequence序号，用于单独统计消费者进度，并且设置到ringBuffer中
    ringBuffer.addGatingSequences(workerPool.getWorkerSequences());
    // 启动workerPool
    //    int nThreads = Runtime.getRuntime().availableProcessors();
    int nThreads = 5;
    ExecutorService executor = Executors.newFixedThreadPool(nThreads);
    workerPool.start(executor);
    CountDownLatch countDownLatch = new CountDownLatch(1);
    // 发送消息
    for (int i = 0; i < 100; i++) {
      new Thread(
              () -> {
                try {
                  countDownLatch.await();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
                Producer producer = new Producer(ringBuffer);
                for (int j = 0; j < 100; j++) {
                  producer.sendData(UUID.randomUUID().toString());
                }
              })
          .start();
    }
    // 创建线程的等待
    Thread.sleep(2_000);
    System.out.println("线程创建完毕，开始生产数据");
    countDownLatch.countDown();
    // 发消息的等待
    Thread.sleep(5_000);
    for (final Consumer consumer : consumerArray) {
      System.out.println(consumer.getCount());
    }
    executor.shutdown();
  }

  private static Consumer[] createConsumers() {
    Consumer[] consumerArray = new Consumer[10];
    for (int i = 0; i < consumerArray.length; i++) {
      consumerArray[i] = new Consumer("C-" + i);
    }
    return consumerArray;
  }

  static class EventExceptionHandler implements ExceptionHandler<Order> {

    @Override
    public void handleEventException(Throwable ex, long sequence, Order event) {}

    @Override
    public void handleOnStartException(Throwable ex) {}

    @Override
    public void handleOnShutdownException(Throwable ex) {}
  }
}
