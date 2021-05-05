package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final Object target;
  private final ProfilingState profilingState;

  // DONE: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock, Object target, ProfilingState profilingState) {
    this.clock = Objects.requireNonNull(clock);
    this.target = Objects.requireNonNull(target);
    this.profilingState = Objects.requireNonNull(profilingState);

  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // DONE: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.

    Object result;
    Instant startTime = null;
    Boolean isProfiled = method.isAnnotationPresent(Profiled.class);

    // Note time only if annotated
    if (isProfiled) {
      startTime = clock.instant();
    }

    // invoke all methods, but record duration only for annotation @Profiled
    try {
        result = method.invoke(target, args);
      } catch (Throwable e) {throw e.getCause();}
      finally {
        // Only record if method is annotated
        if (isProfiled) {
          profilingState.record(target.getClass(), method, Duration.between(startTime, clock.instant()));
        }
      }

    return result;
  }
}
