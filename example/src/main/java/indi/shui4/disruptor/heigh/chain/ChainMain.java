package indi.shui4.disruptor.heigh.chain;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.google.common.base.Stopwatch;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** The type Main. */
@SuppressWarnings("unchecked")
public class ChainMain {
  public static final Random RANDOM = new Random();
  public static final Log log;

  static {
    log = LogFactory.get(ChainMain.class);
  }

  public static void main(String[] args) throws InterruptedException {
    // 关于线程数的设置和下面的5个EventHandler，对于SINGLE来说需要根据EventHandler的数量来设置即5个线程数，
    // 。其原理参考EventProcessor（事件处理器）：由于一个EventHandler对应一个EventProcessor因此对于单消费者来说如果设置少了的话就会卡主，
    // 那多一个EventHandler就要多一个线程？──这样显然不合适......
    ExecutorService consumerExecutor = Executors.newFixedThreadPool(5);
    Disruptor<Trade> disruptor =
        new Disruptor<>(
            Trade::new,
            1024 * 1024,
            consumerExecutor,
            // 多生产者：允许多个线程同时投递消息
            ProducerType.MULTI,
            new BusySpinWaitStrategy());

    EventHandler<Trade> tradeEventHandler1 =
        (event, sequence, endOfBatch) -> {
          ThreadUtil.sleep(10);
          log.info("h1");
          event.setId("1");
        };

    EventHandler<Trade> tradeEventHandler2 =
        (event, sequence, endOfBatch) -> {
          ThreadUtil.sleep(10);
          log.info("h2");
          event.setName("xxxx-" + RANDOM.nextInt());
        };
    EventHandler<Trade> tradeEventHandler3 =
        (event, sequence, endOfBatch) -> {
          log.info("h3:{}", event);
        };

    EventHandler<Trade> tradeEventHandler4 =
        (event, sequence, endOfBatch) -> {
          log.info("h4:{}", event);
        };

    EventHandler<Trade> tradeEventHandler5 =
        (event, sequence, endOfBatch) -> {
          log.info("h5");
        };
    // 串行操作
    //    serial(disruptor, tradeEventHandler1, tradeEventHandler2, tradeEventHandler3);

    // 并行
    //    parallel(disruptor, tradeEventHandler1, tradeEventHandler2, tradeEventHandler3);

    // 菱形操作。1和2并行，3等待1和2的执行再执行
    //    diamond1(disruptor, tradeEventHandler1, tradeEventHandler2, tradeEventHandler3);
    //    diamond2(disruptor, tradeEventHandler1, tradeEventHandler2, tradeEventHandler3);

    // 六边形
    // 这里会发现一个问题：有多少个EventHandler，Disruptor中的线程池就需要多少个线程数，比如这里的handler是5个，那么线程数就必须设置大于这个
    // 数字，不然会卡住，要解决这个问题，需要使用多消费者模式，而在这里使用的是新单消费者
    hexagon(
        disruptor,
        tradeEventHandler1,
        tradeEventHandler2,
        tradeEventHandler3,
        tradeEventHandler4,
        tradeEventHandler5);

    disruptor.start();
    // 生产者线程池
    ExecutorService producerExecutor = Executors.newFixedThreadPool(4);
    EventTranslator<Trade> translator =
        (event, sequence) -> {
          event.setPrice(RANDOM.nextDouble());
        };
    Stopwatch stopwatch = Stopwatch.createStarted();
    int n = 1;
    Runnable runnable =
        () -> {
          // 跟 “  ringBuffer.publish(); ”不同，下面的方式不用获取序列号， 它能自己找到
          for (int j = 0; j < n; j++) {
            disruptor.publishEvent(translator);
          }
        };
    for (int i = 0; i < 1; i++) {
      producerExecutor.execute(runnable);
    }
    executorShutdown(producerExecutor);
    disruptor.shutdown();
    executorShutdown(consumerExecutor);
    System.out.println(stopwatch.stop());
  }
  // 六边形
  private static void hexagon(
      Disruptor<Trade> disruptor,
      EventHandler<Trade> tradeEventHandler1,
      EventHandler<Trade> tradeEventHandler2,
      EventHandler<Trade> tradeEventHandler3,
      EventHandler<Trade> tradeEventHandler4,
      EventHandler<Trade> tradeEventHandler5) {
    // h1与h4是并行的
    disruptor.handleEventsWith(tradeEventHandler1, tradeEventHandler4);
    // h1和h2是串行的
    disruptor.after(tradeEventHandler1).handleEventsWith(tradeEventHandler2);
    // h4和h5是串行的
    disruptor.after(tradeEventHandler4).handleEventsWith(tradeEventHandler5);
    // h2和h5搞定之后，再执行h3
    disruptor.after(tradeEventHandler2, tradeEventHandler5).handleEventsWith(tradeEventHandler3);
  }

  private static void executorShutdown(ExecutorService executorService)
      throws InterruptedException {
    executorService.shutdown();
    executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  private static void diamond1(
      Disruptor<Trade> disruptor,
      EventHandler<Trade> tradeEventHandler1,
      EventHandler<Trade> tradeEventHandler2,
      EventHandler<Trade> tradeEventHandler3) {
    disruptor
        .handleEventsWith(tradeEventHandler1, tradeEventHandler2)
        .handleEventsWith(tradeEventHandler3);
  }

  private static void diamond2(
      Disruptor<Trade> disruptor,
      EventHandler<Trade> tradeEventHandler1,
      EventHandler<Trade> tradeEventHandler2,
      EventHandler<Trade> tradeEventHandler3) {
    disruptor.handleEventsWith(tradeEventHandler1, tradeEventHandler2).then(tradeEventHandler3);
  }

  private static void parallel(
      Disruptor<Trade> disruptor,
      EventHandler<Trade> tradeEventHandler1,
      EventHandler<Trade> tradeEventHandler2,
      EventHandler<Trade> tradeEventHandler3) {
    disruptor.handleEventsWith(tradeEventHandler1, tradeEventHandler2, tradeEventHandler3);
  }

  private static void serial(
      Disruptor<Trade> disruptor,
      EventHandler<Trade> tradeEventHandler1,
      EventHandler<Trade> tradeEventHandler2,
      EventHandler<Trade> tradeEventHandler3) {
    disruptor
        .handleEventsWith(tradeEventHandler1)
        .handleEventsWith(tradeEventHandler2)
        .handleEventsWith(tradeEventHandler3);
  }
}
