package indi.shui4.disruptor.quickstart;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * @author shui4
 * @date 2022/4/13
 * @since 0.0.1
 */
@Setter
@Getter
@Accessors(chain = true)
public class OrderEvent {
  /** 订单价格 */
  private long value;
}
