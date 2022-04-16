package indi.shui4.disruptor.heigh.chain;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shui4
 * @date 2022/4/14
 * @since 0.0.1
 */
@Data
public class Trade {
  private AtomicInteger count = new AtomicInteger();
  private String id;
  private String name;
  private double price;
}
