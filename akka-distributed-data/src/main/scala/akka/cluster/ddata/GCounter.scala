/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.ddata

import akka.cluster.Cluster
import akka.cluster.UniqueAddress
import java.math.BigInteger

object GCounter {
  val empty: GCounter = new GCounter
  def apply(): GCounter = empty
  /**
   * Java API
   */
  def create(): GCounter = empty

  /**
   * Extract the [[GCounter#value]].
   */
  def unapply(c: GCounter): Option[BigInt] = Some(c.value)

  private val Zero = BigInt(0)
}

/**
 * Implements a 'Growing Counter' CRDT, also called a 'G-Counter'.
 *
 * It is described in the paper
 * <a href="http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf">A comprehensive study of Convergent and Commutative Replicated Data Types</a>.
 *
 * A G-Counter is a increment-only counter (inspired by vector clocks) in
 * which only increment and merge are possible. Incrementing the counter
 * adds 1 to the count for the current node. Divergent histories are
 * resolved by taking the maximum count for each node (like a vector
 * clock merge). The value of the counter is the sum of all node counts.
 *
 * This class is immutable, i.e. "modifying" methods return a new instance.
 */
@SerialVersionUID(1L)
final class GCounter private[akka] (
  private[akka] val state: Map[UniqueAddress, BigInt] = Map.empty,
  override val delta:      Option[GCounter]           = None)
  extends DeltaReplicatedData with ReplicatedDataSerialization with RemovedNodePruning with FastMerge {

  import GCounter.Zero

  type T = GCounter
  type D = GCounter

  /**
   * Scala API: Current total value of the counter.
   */
  def value: BigInt = state.values.foldLeft(Zero) { (acc, v) ⇒ acc + v }

  /**
   * Java API: Current total value of the counter.
   */
  def getValue: BigInteger = value.bigInteger

  /**
   * Increment the counter with the delta specified.
   * The delta must be zero or positive.
   */
  def +(delta: Long)(implicit node: Cluster): GCounter = increment(node, delta)

  /**
   * Increment the counter with the delta specified.
   * The delta must be zero or positive.
   */
  def increment(node: Cluster, n: Long = 1): GCounter =
    increment(node.selfUniqueAddress, n)

  /**
   * INTERNAL API
   */
  private[akka] def increment(key: UniqueAddress): GCounter = increment(key, 1)

  /**
   * INTERNAL API
   */
  private[akka] def increment(key: UniqueAddress, n: BigInt): GCounter = {
    require(n >= 0, "Can't decrement a GCounter")
    if (n == 0) this
    else {
      val nextValue = state.get(key) match {
        case Some(v) ⇒ v + n
        case None    ⇒ n
      }
      val newDelta = delta match {
        case Some(d) ⇒ Some(new GCounter(d.state + (key → nextValue)))
        case None    ⇒ Some(new GCounter(Map(key → nextValue)))
      }
      assignAncestor(new GCounter(state + (key → nextValue), newDelta))
    }
  }

  override def merge(that: GCounter): GCounter =
    if ((this eq that) || that.isAncestorOf(this)) this.clearAncestor()
    else if (this.isAncestorOf(that)) that.clearAncestor()
    else {
      var merged = that.state
      for ((key, thisValue) ← state) {
        val thatValue = merged.getOrElse(key, Zero)
        if (thisValue > thatValue)
          merged = merged.updated(key, thisValue)
      }
      clearAncestor()
      val newDelta = delta match {
        case Some(d1) ⇒ that.delta match {
          case Some(d2) ⇒ Some(d1.merge(d2))
          case None     ⇒ delta
        }
        case None ⇒ that.delta
      }
      new GCounter(merged, newDelta)
    }

  override def mergeDelta(d: GCounter): GCounter = merge(d)

  override def clearDelta: GCounter = new GCounter(state)

  // FIXME handle pruning for deltas

  override def needPruningFrom(removedNode: UniqueAddress): Boolean =
    state.contains(removedNode)

  override def prune(removedNode: UniqueAddress, collapseInto: UniqueAddress): GCounter =
    state.get(removedNode) match {
      case Some(value) ⇒ new GCounter(state - removedNode).increment(collapseInto, value)
      case None        ⇒ this
    }

  override def pruningCleanup(removedNode: UniqueAddress): GCounter =
    new GCounter(state - removedNode)

  // this class cannot be a `case class` because we need different `unapply`

  override def toString: String = s"GCounter($value)"

  override def equals(o: Any): Boolean = o match {
    case other: GCounter ⇒ state == other.state
    case _               ⇒ false
  }

  override def hashCode: Int = state.hashCode

}

object GCounterKey {
  def create(id: String): Key[GCounter] = GCounterKey(id)
}

@SerialVersionUID(1L)
final case class GCounterKey(_id: String) extends Key[GCounter](_id) with ReplicatedDataSerialization
