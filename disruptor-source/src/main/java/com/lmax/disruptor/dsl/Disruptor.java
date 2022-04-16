/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor.dsl;

import com.lmax.disruptor.*;
import com.lmax.disruptor.util.Util;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A DSL-style API for setting up the disruptor pattern around a ring buffer (aka the Builder
 * pattern).
 *
 * <p>A simple example of setting up the disruptor with two event handlers that must process events
 * in order:
 *
 * <pre>
 * <code>Disruptor&lt;MyEvent&gt; disruptor = new Disruptor&lt;MyEvent&gt;(MyEvent.FACTORY, 32, Executors.newCachedThreadPool());
 * EventHandler&lt;MyEvent&gt; handler1 = new EventHandler&lt;MyEvent&gt;() { ... };
 * EventHandler&lt;MyEvent&gt; handler2 = new EventHandler&lt;MyEvent&gt;() { ... };
 * disruptor.handleEventsWith(handler1);
 * disruptor.after(handler1).handleEventsWith(handler2);
 *
 * RingBuffer ringBuffer = disruptor.start();</code>
 * </pre>
 *
 * @param <T> the type of event used.
 */
public class Disruptor<T> {
  private final ConsumerRepository<T> consumerRepository = new ConsumerRepository<T>();
  private final Executor executor;
  private final RingBuffer<T> ringBuffer;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private ExceptionHandler<? super T> exceptionHandler = new ExceptionHandlerWrapper<T>();

  /**
   * Create a new Disruptor. Will default to {@link com.lmax.disruptor.BlockingWaitStrategy} and
   * {@link ProducerType}.MULTI
   *
   * @deprecated Use a {@link ThreadFactory} instead of an {@link Executor} as a the ThreadFactory
   *     is able to report errors when it is unable to construct a thread to run a producer.
   * @param eventFactory the factory to create events in the ring buffer.
   * @param ringBufferSize the size of the ring buffer.
   * @param executor an {@link Executor} to execute event processors.
   */
  @Deprecated
  public Disruptor(
      final EventFactory<T> eventFactory, final int ringBufferSize, final Executor executor) {
    this(RingBuffer.createMultiProducer(eventFactory, ringBufferSize), executor);
  }

  /** Private constructor helper */
  private Disruptor(final RingBuffer<T> ringBuffer, final Executor executor) {
    this.ringBuffer = ringBuffer;
    this.executor = executor;
  }

  /**
   * Create a new Disruptor.
   *
   * @deprecated Use a {@link ThreadFactory} instead of an {@link Executor} as a the ThreadFactory
   *     is able to report errors when it is unable to construct a thread to run a producer.
   * @param eventFactory the factory to create events in the ring buffer.
   * @param ringBufferSize the size of the ring buffer, must be power of 2.
   * @param executor an {@link Executor} to execute event processors.
   * @param producerType the claim strategy to use for the ring buffer.
   * @param waitStrategy the wait strategy to use for the ring buffer.
   */
  @Deprecated
  public Disruptor(
      final EventFactory<T> eventFactory,
      final int ringBufferSize,
      final Executor executor,
      final ProducerType producerType,
      final WaitStrategy waitStrategy) {
    this(RingBuffer.create(producerType, eventFactory, ringBufferSize, waitStrategy), executor);
  }

  /**
   * Create a new Disruptor. Will default to {@link com.lmax.disruptor.BlockingWaitStrategy} and
   * {@link ProducerType}.MULTI
   *
   * @param eventFactory the factory to create events in the ring buffer.
   * @param ringBufferSize the size of the ring buffer.
   * @param threadFactory a {@link ThreadFactory} to create threads to for processors.
   */
  public Disruptor(
      final EventFactory<T> eventFactory,
      final int ringBufferSize,
      final ThreadFactory threadFactory) {
    this(
        RingBuffer.createMultiProducer(eventFactory, ringBufferSize),
        new BasicExecutor(threadFactory));
  }

  /**
   * Create a new Disruptor.
   *
   * @param eventFactory the factory to create events in the ring buffer.
   * @param ringBufferSize the size of the ring buffer, must be power of 2.
   * @param threadFactory a {@link ThreadFactory} to create threads for processors.
   * @param producerType the claim strategy to use for the ring buffer.
   * @param waitStrategy the wait strategy to use for the ring buffer.
   */
  public Disruptor(
      final EventFactory<T> eventFactory,
      final int ringBufferSize,
      final ThreadFactory threadFactory,
      final ProducerType producerType,
      final WaitStrategy waitStrategy) {
    this(
        RingBuffer.create(producerType, eventFactory, ringBufferSize, waitStrategy),
        new BasicExecutor(threadFactory));
  }

  /**
   * Create a group of event handlers to be used as a dependency. For example if the handler <code>A
   * </code> must process events before handler <code>B</code>:
   *
   * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
   *
   * @param handlers the event handlers, previously set up with {@link
   *     #handleEventsWith(EventHandler[])}, that will form the barrier for subsequent handlers or
   *     processors.
   * @return an {@link EventHandlerGroup} that can be used to setup a dependency barrier over the
   *     specified event handlers.
   */
  @SuppressWarnings("varargs")
  public EventHandlerGroup<T> after(final EventHandler<T>... handlers) {
    final Sequence[] sequences = new Sequence[handlers.length];
    for (int i = 0, handlersLength = handlers.length; i < handlersLength; i++) {
      sequences[i] = consumerRepository.getSequenceFor(handlers[i]);
    }

    return new EventHandlerGroup<T>(this, consumerRepository, sequences);
  }

  /**
   * Create a group of event processors to be used as a dependency.
   *
   * @param processors the event processors, previously set up with {@link
   *     #handleEventsWith(EventProcessor...)}, that will form the barrier for subsequent handlers
   *     or processors.
   * @return an {@link EventHandlerGroup} that can be used to setup a {@link SequenceBarrier} over
   *     the specified event processors.
   * @see #after(EventHandler[])
   */
  public EventHandlerGroup<T> after(final EventProcessor... processors) {
    for (final EventProcessor processor : processors) {
      consumerRepository.add(processor);
    }

    return new EventHandlerGroup<T>(this, consumerRepository, Util.getSequencesFor(processors));
  }

  /**
   * Get the event for a given sequence in the RingBuffer.
   *
   * @param sequence for the event.
   * @return event for the sequence.
   * @see RingBuffer#get(long)
   */
  public T get(final long sequence) {
    return ringBuffer.get(sequence);
  }

  /**
   * Get the {@link SequenceBarrier} used by a specific handler. Note that the {@link
   * SequenceBarrier} may be shared by multiple event handlers.
   *
   * @param handler the handler to get the barrier for.
   * @return the SequenceBarrier used by <i>handler</i>.
   */
  public SequenceBarrier getBarrierFor(final EventHandler<T> handler) {
    return consumerRepository.getBarrierFor(handler);
  }

  /**
   * The capacity of the data structure to hold entries.
   *
   * @return the size of the RingBuffer.
   * @see com.lmax.disruptor.Sequencer#getBufferSize()
   */
  public long getBufferSize() {
    return ringBuffer.getBufferSize();
  }

  /**
   * Get the value of the cursor indicating the published sequence.
   *
   * @return value of the cursor for events that have been published.
   */
  public long getCursor() {
    return ringBuffer.getCursor();
  }

  /**
   * The {@link RingBuffer} used by this Disruptor. This is useful for creating custom event
   * processors if the behaviour of {@link BatchEventProcessor} is not suitable.
   *
   * @return the ring buffer used by this Disruptor.
   */
  public RingBuffer<T> getRingBuffer() {
    return ringBuffer;
  }

  /**
   * Gets the sequence value for the specified event handlers.
   *
   * @param b1 eventHandler to get the sequence for.
   * @return eventHandler's sequence
   */
  public long getSequenceValueFor(final EventHandler<T> b1) {
    return consumerRepository.getSequenceFor(b1).get();
  }

  /**
   * Set up event handlers to handle events from the ring buffer. These handlers will process events
   * as soon as they become available, in parallel.
   *
   * <p>This method can be used as the start of a chain. For example if the handler <code>A</code>
   * must process events before handler <code>B</code>:
   *
   * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
   *
   * @param handlers the event handlers that will process events.
   * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
   */
  @SuppressWarnings("varargs")
  public EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers) {
    return createEventProcessors(new Sequence[0], handlers);
  }

  EventHandlerGroup<T> createEventProcessors(
      final Sequence[] barrierSequences, final EventHandler<? super T>[] eventHandlers) {
    checkNotStarted();

    final Sequence[] processorSequences = new Sequence[eventHandlers.length];
    final SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);

    for (int i = 0, eventHandlersLength = eventHandlers.length; i < eventHandlersLength; i++) {
      final EventHandler<? super T> eventHandler = eventHandlers[i];

      final BatchEventProcessor<T> batchEventProcessor =
          new BatchEventProcessor<T>(ringBuffer, barrier, eventHandler);

      if (exceptionHandler != null) {
        batchEventProcessor.setExceptionHandler(exceptionHandler);
      }

      consumerRepository.add(batchEventProcessor, eventHandler, barrier);
      processorSequences[i] = batchEventProcessor.getSequence();
    }

    updateGatingSequencesForNextInChain(barrierSequences, processorSequences);

    return new EventHandlerGroup<T>(this, consumerRepository, processorSequences);
  }

  private void checkNotStarted() {
    if (started.get()) {
      throw new IllegalStateException("All event handlers must be added before calling starts.");
    }
  }

  private void updateGatingSequencesForNextInChain(
      final Sequence[] barrierSequences, final Sequence[] processorSequences) {
    if (processorSequences.length > 0) {
      ringBuffer.addGatingSequences(processorSequences);
      for (final Sequence barrierSequence : barrierSequences) {
        ringBuffer.removeGatingSequence(barrierSequence);
      }
      consumerRepository.unMarkEventProcessorsAsEndOfChain(barrierSequences);
    }
  }

  /**
   * Set up custom event processors to handle events from the ring buffer. The Disruptor will
   * automatically start these processors when {@link #start()} is called.
   *
   * <p>This method can be used as the start of a chain. For example if the handler <code>A</code>
   * must process events before handler <code>B</code>:
   *
   * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
   *
   * <p>Since this is the start of the chain, the processor factories will always be passed an empty
   * <code>Sequence</code> array, so the factory isn't necessary in this case. This method is
   * provided for consistency with {@link
   * EventHandlerGroup#handleEventsWith(EventProcessorFactory...)} and {@link
   * EventHandlerGroup#then(EventProcessorFactory...)} which do have barrier sequences to provide.
   *
   * @param eventProcessorFactories the event processor factories to use to create the event
   *     processors that will process events.
   * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
   */
  public EventHandlerGroup<T> handleEventsWith(
      final EventProcessorFactory<T>... eventProcessorFactories) {
    final Sequence[] barrierSequences = new Sequence[0];
    return createEventProcessors(barrierSequences, eventProcessorFactories);
  }

  EventHandlerGroup<T> createEventProcessors(
      final Sequence[] barrierSequences, final EventProcessorFactory<T>[] processorFactories) {
    final EventProcessor[] eventProcessors = new EventProcessor[processorFactories.length];
    for (int i = 0; i < processorFactories.length; i++) {
      eventProcessors[i] = processorFactories[i].createEventProcessor(ringBuffer, barrierSequences);
    }

    return handleEventsWith(eventProcessors);
  }

  /**
   * Set up custom event processors to handle events from the ring buffer. The Disruptor will
   * automatically start this processors when {@link #start()} is called.
   *
   * <p>This method can be used as the start of a chain. For example if the processor <code>A</code>
   * must process events before handler <code>B</code>:
   *
   * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
   *
   * @param processors the event processors that will process events.
   * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
   */
  public EventHandlerGroup<T> handleEventsWith(final EventProcessor... processors) {
    for (final EventProcessor processor : processors) {
      consumerRepository.add(processor);
    }

    final Sequence[] sequences = new Sequence[processors.length];
    for (int i = 0; i < processors.length; i++) {
      sequences[i] = processors[i].getSequence();
    }

    ringBuffer.addGatingSequences(sequences);

    return new EventHandlerGroup<T>(this, consumerRepository, Util.getSequencesFor(processors));
  }

  /**
   * Set up a {@link WorkerPool} to distribute an event to one of a pool of work handler threads.
   * Each event will only be processed by one of the work handlers. The Disruptor will automatically
   * start this processors when {@link #start()} is called.
   *
   * @param workHandlers the work handlers that will process events.
   * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
   */
  @SuppressWarnings("varargs")
  public EventHandlerGroup<T> handleEventsWithWorkerPool(final WorkHandler<T>... workHandlers) {
    return createWorkerPool(new Sequence[0], workHandlers);
  }

  EventHandlerGroup<T> createWorkerPool(
      final Sequence[] barrierSequences, final WorkHandler<? super T>[] workHandlers) {
    final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier(barrierSequences);
    final WorkerPool<T> workerPool =
        new WorkerPool<T>(ringBuffer, sequenceBarrier, exceptionHandler, workHandlers);

    consumerRepository.add(workerPool, sequenceBarrier);

    final Sequence[] workerSequences = workerPool.getWorkerSequences();

    updateGatingSequencesForNextInChain(barrierSequences, workerSequences);

    return new EventHandlerGroup<T>(this, consumerRepository, workerSequences);
  }

  /**
   * Override the default exception handler for a specific handler.
   *
   * <pre>disruptorWizard.handleExceptionsIn(eventHandler).with(exceptionHandler);</pre>
   *
   * @param eventHandler the event handler to set a different exception handler for.
   * @return an ExceptionHandlerSetting dsl object - intended to be used by chaining the with method
   *     call.
   */
  public ExceptionHandlerSetting<T> handleExceptionsFor(final EventHandler<T> eventHandler) {
    return new ExceptionHandlerSetting<T>(eventHandler, consumerRepository);
  }

  /**
   * Specify an exception handler to be used for any future event handlers.
   *
   * <p>Note that only event handlers set up after calling this method will use the exception
   * handler.
   *
   * @param exceptionHandler the exception handler to use for any future {@link EventProcessor}.
   * @deprecated This method only applies to future event handlers. Use setDefaultExceptionHandler
   *     instead which applies to existing and new event handlers.
   */
  public void handleExceptionsWith(final ExceptionHandler<? super T> exceptionHandler) {
    this.exceptionHandler = exceptionHandler;
  }

  /**
   * Publish an event to the ring buffer.
   *
   * @param eventTranslator the translator that will load data into the event.
   */
  public void publishEvent(final EventTranslator<T> eventTranslator) {
    ringBuffer.publishEvent(eventTranslator);
  }

  /**
   * Publish an event to the ring buffer.
   *
   * @param <A> Class of the user supplied argument.
   * @param eventTranslator the translator that will load data into the event.
   * @param arg A single argument to load into the event
   */
  public <A> void publishEvent(final EventTranslatorOneArg<T, A> eventTranslator, final A arg) {
    ringBuffer.publishEvent(eventTranslator, arg);
  }

  /**
   * Publish an event to the ring buffer.
   *
   * @param <A> Class of the user supplied argument.
   * @param <B> Class of the user supplied argument.
   * @param eventTranslator the translator that will load data into the event.
   * @param arg0 The first argument to load into the event
   * @param arg1 The second argument to load into the event
   */
  public <A, B> void publishEvent(
      final EventTranslatorTwoArg<T, A, B> eventTranslator, final A arg0, final B arg1) {
    ringBuffer.publishEvent(eventTranslator, arg0, arg1);
  }

  /**
   * Publish an event to the ring buffer.
   *
   * @param eventTranslator the translator that will load data into the event.
   * @param <A> Class of the user supplied argument.
   * @param <B> Class of the user supplied argument.
   * @param <C> Class of the user supplied argument.
   * @param arg0 The first argument to load into the event
   * @param arg1 The second argument to load into the event
   * @param arg2 The third argument to load into the event
   */
  public <A, B, C> void publishEvent(
      final EventTranslatorThreeArg<T, A, B, C> eventTranslator,
      final A arg0,
      final B arg1,
      final C arg2) {
    ringBuffer.publishEvent(eventTranslator, arg0, arg1, arg2);
  }

  /**
   * Publish a batch of events to the ring buffer.
   *
   * @param <A> Class of the user supplied argument.
   * @param eventTranslator the translator that will load data into the event.
   * @param arg An array single arguments to load into the events. One Per event.
   */
  public <A> void publishEvents(final EventTranslatorOneArg<T, A> eventTranslator, final A[] arg) {
    ringBuffer.publishEvents(eventTranslator, arg);
  }

  /**
   * Specify an exception handler to be used for event handlers and worker pools created by this
   * Disruptor.
   *
   * <p>The exception handler will be used by existing and future event handlers and worker pools
   * created by this Disruptor instance.
   *
   * @param exceptionHandler the exception handler to use.
   */
  @SuppressWarnings("unchecked")
  public void setDefaultExceptionHandler(final ExceptionHandler<? super T> exceptionHandler) {
    checkNotStarted();
    if (!(this.exceptionHandler instanceof ExceptionHandlerWrapper)) {
      throw new IllegalStateException(
          "setDefaultExceptionHandler can not be used after handleExceptionsWith");
    }
    ((ExceptionHandlerWrapper<T>) this.exceptionHandler).switchTo(exceptionHandler);
  }

  /**
   * Waits until all events currently in the disruptor have been processed by all event processors
   * and then halts the processors. It is critical that publishing to the ring buffer has stopped
   * before calling this method, otherwise it may never return.
   *
   * <p>This method will not shutdown the executor, nor will it await the final termination of the
   * processor threads.
   */
  public void shutdown() {
    try {
      shutdown(-1, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      exceptionHandler.handleOnShutdownException(e);
    }
  }

  /**
   * Waits until all events currently in the disruptor have been processed by all event processors
   * and then halts the processors.
   *
   * <p>This method will not shutdown the executor, nor will it await the final termination of the
   * processor threads.
   *
   * @param timeout the amount of time to wait for all events to be processed. <code>-1</code> will
   *     give an infinite timeout
   * @param timeUnit the unit the timeOut is specified in
   * @throws TimeoutException if a timeout occurs before shutdown completes.
   */
  public void shutdown(final long timeout, final TimeUnit timeUnit) throws TimeoutException {
    final long timeOutAt = System.currentTimeMillis() + timeUnit.toMillis(timeout);
    while (hasBacklog()) {
      if (timeout >= 0 && System.currentTimeMillis() > timeOutAt) {
        throw TimeoutException.INSTANCE;
      }
      // Busy spin
    }
    halt();
  }

  /** Confirms if all messages have been consumed by all event processors */
  private boolean hasBacklog() {
    final long cursor = ringBuffer.getCursor();
    for (final Sequence consumer : consumerRepository.getLastSequenceInChain(false)) {
      if (cursor > consumer.get()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Calls {@link EventProcessor#halt()} on all of the event processors created via this disruptor.
   */
  public void halt() {
    for (final ConsumerInfo consumerInfo : consumerRepository) {
      consumerInfo.halt();
    }
  }

  /**
   * Starts the event processors and returns the fully configured ring buffer.
   *
   * <p>The ring buffer is set up to prevent overwriting any entry that is yet to be processed by
   * the slowest event processor.
   *
   * <p>This method must only be called once after all event processors have been added.
   *
   * @return the configured ring buffer.
   */
  public RingBuffer<T> start() {
    checkOnlyStartedOnce();
    for (final ConsumerInfo consumerInfo : consumerRepository) {
      consumerInfo.start(executor);
    }

    return ringBuffer;
  }

  private void checkOnlyStartedOnce() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Disruptor.start() must only be called once.");
    }
  }

  @Override
  public String toString() {
    return "Disruptor{"
        + "ringBuffer="
        + ringBuffer
        + ", started="
        + started
        + ", executor="
        + executor
        + '}';
  }
}
