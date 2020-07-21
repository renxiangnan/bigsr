package org.bigsr.fwk

import java.util.concurrent.{ForkJoinPool, ForkJoinTask, ForkJoinWorkerThread, RecursiveTask}

import scala.util.DynamicVariable

/**
  * @author xiangnan ren
  */
package object common {
  type Fact = Seq[String]

  val forkJoinPool = new ForkJoinPool
  val scheduler =
    new DynamicVariable[TaskScheduler](new DefaultTaskScheduler)

  def task[T](body: => T): ForkJoinTask[T] = {
    scheduler.value.schedule(body)
  }

  abstract class TaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T]

    def parallel[A, B](taskA: => A,
                       taskB: => B): (A, B) = {
      val right = task {
        taskB
      }

      val left = taskA
      (left, right.join())
    }
  }

  class DefaultTaskScheduler extends TaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T] = {
      val t = new RecursiveTask[T] {
        def compute = body
      }
      Thread.currentThread match {
        case _: ForkJoinWorkerThread =>
          t.fork()
        case _ => forkJoinPool.execute(t)
      }
      t
    }
  }

  object Utils {
    def displayExecutionTime[T](task: => T): Unit = {
      val tStart = System.nanoTime()
      task
      println(s"The execution time is " +
        s"${(System.nanoTime() - tStart) / 1e6} ms.")
    }

    def getExecutionTime[T](task: => T): Double = {
      val tStart = System.nanoTime()
      task
      (System.nanoTime() - tStart) / 1e6
    }
  }

}
