/**
 * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.cluster.ddata

import scala.concurrent.duration._

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit._
import com.typesafe.config.ConfigFactory

object ReplicatorDeltaSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    """))

  testTransport(on = true)

}

class ReplicatorDeltaSpecMultiJvmNode1 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode2 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode3 extends ReplicatorDeltaSpec
class ReplicatorDeltaSpecMultiJvmNode4 extends ReplicatorDeltaSpec

class ReplicatorDeltaSpec extends MultiNodeSpec(ReplicatorDeltaSpec) with STMultiNodeSpec with ImplicitSender {
  import ReplicatorDeltaSpec._
  import Replicator._

  override def initialParticipants = roles.size

  implicit val cluster = Cluster(system)
  val replicator = system.actorOf(Replicator.props(
    ReplicatorSettings(system).withGossipInterval(1.second).withMaxDeltaElements(10)), "replicator")

  val KeyA = GCounterKey("A")
  val KeyB = GCounterKey("B")

  var afterCounter = 0
  def enterBarrierAfterTestStep(): Unit = {
    afterCounter += 1
    enterBarrier("after-" + afterCounter)
  }

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster join node(to).address
    }
    enterBarrier(from.name + "-joined")
  }

  "Cluster delta-CRDT" must {
    "propagate deltas" in {
      join(first, first)
      join(second, first)
      join(third, first)
      join(fourth, first)

      within(15.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(4))
        }
      }
      enterBarrier("ready")

      runOn(first) {
        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ + 1)
      }
      enterBarrier("updated-1")

      within(5.seconds) {
        awaitAssert {
          replicator ! Get(KeyA, ReadLocal)
          expectMsgType[GetSuccess[GCounter]].dataValue.getValue.intValue should be(1)
        }
      }
      enterBarrier("verified-1")

      runOn(first) {
        replicator ! Update(KeyA, GCounter.empty, WriteLocal)(_ + 1)
      }
      enterBarrier("updated-2")

      within(5.seconds) {
        awaitAssert {
          replicator ! Get(KeyA, ReadLocal)
          expectMsgType[GetSuccess[GCounter]].dataValue.getValue.intValue should be(2)
        }
      }
      enterBarrier("verified-2")

      Thread.sleep(10000) // FIXME checking logs

      enterBarrierAfterTestStep()
    }
  }

}

