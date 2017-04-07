// Copyright (c) 2017 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract class LustreFsMergeThread<T,K,V> extends Thread {
  
  private static final Log LOG = LogFactory.getLog(LustreFsMergeThread.class);

  private AtomicInteger numPending = new AtomicInteger(0);
  private LinkedList<List<T>> pendingToBeMerged;
  private final ExceptionReporter reporter;
  private boolean closed = false;
  private final int mergeFactor;
  
  public LustreFsMergeThread(int mergeFactor, ExceptionReporter reporter) {
    this.pendingToBeMerged = new LinkedList<List<T>>();
    this.mergeFactor = mergeFactor;
    this.reporter = reporter;
  }
  
  public synchronized void close() throws InterruptedException {
    closed = true;
    waitForMerge();
    interrupt();
  }

  public void startMerge(Set<T> inputs) {
    if (!closed) {
      numPending.incrementAndGet();
      List<T> toMergeInputs = new ArrayList<T>();
      Iterator<T> iter=inputs.iterator();
      for (int ctr = 0; iter.hasNext() && ctr < mergeFactor; ++ctr) {
        toMergeInputs.add(iter.next());
        iter.remove();
      }
      LOG.info(getName() + ": Starting merge with " + toMergeInputs.size() + 
               " segments, while ignoring " + inputs.size() + " segments");
      synchronized(pendingToBeMerged) {
        pendingToBeMerged.addLast(toMergeInputs);
        pendingToBeMerged.notifyAll();
      }
    }
  }

  public synchronized void waitForMerge() throws InterruptedException {
    while (numPending.get() > 0) {
      wait();
    }
  }

  public void run() {
    while (true) {
      List<T> inputs = null;
      try {
        // Wait for notification to start the merge...
        synchronized (pendingToBeMerged) {
          while(pendingToBeMerged.size() <= 0) {
            pendingToBeMerged.wait();
          }
          // Pickup the inputs to merge.
          inputs = pendingToBeMerged.removeFirst();
        }

        // Merge
        merge(inputs);
      } catch (InterruptedException ie) {
        numPending.set(0);
        return;
      } catch(Throwable t) {
        numPending.set(0);
        reporter.reportException(t);
        return;
      } finally {
        synchronized (this) {
          numPending.decrementAndGet();
          notifyAll();
        }
      }
    }
  }

  public abstract void merge(List<T> inputs) throws IOException;
}
