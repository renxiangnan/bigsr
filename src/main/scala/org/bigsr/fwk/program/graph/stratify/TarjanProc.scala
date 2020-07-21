package org.bigsr.fwk.program.graph.stratify

import org.bigsr.fwk.program.graph.{DependencyGraph, Vertex}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * @author xiangnan ren
  */
object TarjanProc {
  type NumLabel = Map[Vertex, Int]
  type DenomLabel = Map[Vertex, Int]

  def stratify(graph: DependencyGraph): StratifiedGraph = {
    val initVertex = graph.vertices.head
    val reversedGraph = graph.reversedGraph
    val denomLabel = label(initVertex,
      updateNumerator(initVertex, initState(reversedGraph)))._2

    createSCCs(graph, denomLabel)
  }

  private def initState(g: DependencyGraph): State =
    State(
      graph = g,
      counter = -1,
      visited = g.vertices.map((_, false)).toMap,
      numerator = Map.empty[Vertex, Int],
      denominator = Map.empty[Vertex, Int],
      stack = Nil)

  private def updateNumerator(vertex: Vertex,
                              state: State): State =
    state.copy(
      counter = state.counter + 1,
      visited = state.visited.updated(vertex, true),
      numerator = state.numerator.updated(vertex, state.counter + 1),
      stack = vertex :: state.stack)

  private def updateDenominator(vertex: Vertex,
                                state: State): State =
    state.copy(
      counter = state.counter + 1,
      denominator = state.denominator.updated(vertex, state.counter + 1),
      stack = state.stack.drop(1))

  @tailrec
  private def label(vertex: Vertex,
                    state: State): (NumLabel, DenomLabel) =
    state.graph.edges.find(edge =>
      edge.src.equals(vertex) && !state.visited(edge.dst)) match {
      case Some(edge) => label(edge.dst, updateNumerator(edge.dst, state))

      case None =>
        val newState = updateDenominator(vertex, state)
        if (newState.stack.nonEmpty)
          label(newState.stack.head, newState)
        else {
          state.graph.vertices.
            find(vertex => !state.visited(vertex)) match {
            case Some(v) => val _newState = updateNumerator(v, newState)
              label(v, _newState)

            case None => (newState.numerator, newState.denominator)
          }
        }
    }

  private def createSCCs(graph: DependencyGraph,
                         denomLabel: DenomLabel): StratifiedGraph = {
    val sortedVertices: Seq[(Vertex, Int)] = denomLabel.toSeq.
      sortWith((pair1, pair2) => pair1._2 > pair2._2)
    val initVertices = {
      val set = mutable.HashMap[Vertex, Boolean]()
      graph.vertices.foreach(v => set.+=(v -> false))
      set
    }

    def createSCC(start: Vertex,
                  visitedVertices: mutable.HashMap[Vertex, Boolean],
                  vertices: Set[Vertex],
                  stack: List[Vertex]): Set[Vertex] = {
      graph.edges.find(e =>
        e.src.equals(start) && !visitedVertices(e.dst)) match {
        case Some(edge) =>
          createSCC(
            edge.dst,
            visitedVertices.+=(edge.dst -> true),
            vertices + edge.dst,
            start :: (edge.dst :: stack))

        case None => if (stack.nonEmpty) {
          createSCC(
            stack.head,
            visitedVertices,
            vertices + start,
            stack.drop(1))
        } else vertices
      }
    }

    val sccs = for (vp <- sortedVertices if !initVertices(vp._1))
      yield {
        val scc = Set[Vertex]().empty
        createSCC(vp._1, initVertices.+=(vp._1 -> true), scc + vp._1, Nil)
      }

    StratifiedGraph(graph, sccs.toSet, denomLabel)
  }

  private case class State(graph: DependencyGraph,
                           counter: Int,
                           visited: Map[Vertex, Boolean],
                           numerator: NumLabel,
                           denominator: DenomLabel,
                           stack: List[Vertex])

}
