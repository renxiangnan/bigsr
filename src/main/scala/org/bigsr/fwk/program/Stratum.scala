package org.bigsr.fwk.program

import org.bigsr.fwk.program.rule.Rule

/**
  * @author xiangnan ren
  */
case class Stratum(id: Int,
                   rules: Set[Rule]) {
  val isRecursive: Boolean = {
    val headAppearance = rules.collect { case r: Rule =>
      r.head.getPredicate
    }.size
    val bodyAppearance = rules.collect { case r: Rule =>
      r.subgoals
    }.flatten.size

    if (headAppearance > 1) true
    else if (headAppearance == 1 && bodyAppearance == 0) false
    else true
  }

}

