#!/usr/bin/env python3
"""
Library for executing python objects remotely.


    Machine A              Machine B

    RoiPeer             RoiPeer


    ROI_A = RoiPeer()                            ROI_B = RoiPeer()
    m = ROI.distribute( myObject )
                          +----------> pickle -->     store myObject
    m <--- communication object

    m.method(args, onSuccess, onFailure)
         subscribe to response topic
         ---> pickle --> mqtt -->                     r = myObject.method(args)
         callback                        <--          publish(response_topic,r)

"""
import paho.mqtt.client as mqtt
import pickle
import threading
import uuid
import traceback
import os
import logging
import json
import time
import sys

MQTT_BROKER_IP = "localhost"
MQTT_BROKER_PORT = 1883
SYS_LOAD_SAMPLE_INTERVAL = 5
MIN_WORKERS = 3

ROI_TOPIC_EXCEPTION = "roi/exception"
ROI_TOPIC_REGISTER = "roi/register/%(peerId)s"
ROI_TOPIC_UNREGISTER = "roi/unregister/%(oid)s"
ROI_TOPIC_OBJECT_READY = "roi/objectReady/"
ROI_TOPIC_METHOD = "roi/call/%(oid)s/%(method)s"
ROI_TOPIC_METHOD_FAIL = "roi/call/%(oid)s/fail"

ROI_TOPIC_LOAD = "roi/peerLoad"




class RoiPeer(object):
    "A place were remote objects live and do work"

    def subscribe(self, topic, cb):
        logging.debug("%s subscribing to %s" % (self.__class__.__name__, topic))        
        self._mqttc.subscribe(topic)
        self._handlers[topic] = cb

    def unsubscribe(self, topic):
        logging.debug("%s unsubscribing from %s" % (self.__class__.__name__, topic))        
        self._mqttc.unsubscribe(topic)
        if topic in self._handlers:
            del self._handlers[topic]

    def on_message(self, client, userdata, msg):
        logging.debug("RoiPeer: on_message topic=%s data=%s" % (msg.topic,repr(msg.payload)))
        if msg.topic in self._handlers:
            try:
                self._handlers[msg.topic](client, msg)
            except BaseException:
                logging.error(traceback.format_exc())

    def on_connect(self, client, userdata, flags, rc):
        self.connected = threading.Event()

    """ Base class for all remote objects.
    """

    def __init__(self, **kwargs):
        self._on_ready = kwargs.get('onReady')
        self._peerId = kwargs.get('peerId', os.uname().nodename)
        self._unhandled_exc_handler = kwargs.get('exceptionHandler')
        self._mqttc = mqtt.Client()
        self._mqttc.on_connect = self.on_connect
        self._mqttc.on_message = self.on_message
        self._mqttc.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT, 60)
        self._mqttc.loop_start()
        self._store = {}
        self._handlers = {}
        self.connected = threading.Event()

    def __del__(self):
        self._mqttc.loop_stop()


class RoiSurrogate:
    def __init__(self, obj, peer):
        self.obj = obj
        self.peer = peer

    def __getattr__(self, name):
        # operator overload!!!!
        #  objCtrl = RoiControlPeer.deploy( some_object, onReady, onFailure )
        # objCtrl.foo -> __getattr__( 'foo' ) -> remotecall to obj.foo ->
        # result routed back
        logging.debug("__getattr__ %s" % name)

        if not hasattr(self.obj, name):
            return TypeError, "No such method %s" % name
        if not callable(getattr(self.obj, name)):
            return TypeError, "Attribute %s must be callable" % name

        def remotecall(*args, **kwargs):
            # execute remote procedure call, if user specifies a reply function
            # setup a deuplex path to capture the function result and route back
            # to user, otherwise its send and forget.
            req = {
                "method": name,
                "args": args,
                "kwargs": kwargs
            }

            reply = kwargs.get('reply')
            if reply:
                del kwargs['reply']
                resp_topic = "roi/%s" % uuid.uuid1().hex
                req['resp_topic'] = resp_topic

                def reply_handler(client, msg):
                    jobj = json.loads(msg.payload.decode())
                    logging.debug("surrogate: reply_handler %s" % str(msg.payload))         

                    reply(jobj)
                    logging.debug("surrogate: unsubscribe to " + resp_topic) 
                    self.peer.unsubscribe(resp_topic)

                # handle function call result
                logging.debug("surrogate: subscribe to " + resp_topic) 
                self.peer.subscribe(resp_topic, reply_handler)

            topic = ROI_TOPIC_METHOD % {
                'oid': self.obj.oid,
                'method': name
            }
             
                        
            # call peer.
            logging.debug("surrogate: publishing to topic %s %s" % (topic,str(req)))         
            self.peer._mqttc.publish(topic, payload=json.dumps(req))

        # return a method that will execute a remote procedure call.
        return remotecall


class RoiRecord(object):
    def __init__(self, obj, ready, failure):
        self.obj = obj
        self.ready = ready
        self.failure = failure


class RoiControlPeer(RoiPeer):
    "Master controller of remote objects"

    def unhandled_exception(self, client, msg):
        "Remote call throws exception"
        if self._unhandled_exc_handler:
            self._unhandled_exc_handler(client, msg)
        else:
            (obj, method, stacktrace) = json.loads(msg.payload.decode())
            logging.error(stacktrace)

    def load_handler(self, client, msg):
        # worker nodes sending us thier periodic load factors
        m = json.loads(msg.payload.decode())

        # compute score for this worker node.
        tm = 1
        if m['peerId'] in self._workers:
            last = self._workers[m['peerId']]['last']
            past_due = time.time() - last - SYS_LOAD_SAMPLE_INTERVAL
            if past_due > 0.5:
                tm += (past_due - 0.5)
        score = 0
        for (i, w) in enumerate([1, 5, 15]):
            score += (i * w)
        # add a past due penalty if needed
        score *= tm

        self._workers[m['peerId']] = {
            'score': score,
            'last': time.time(),
            'peerId': m['peerId']
        }

        if len(self._workers) >= self._min_workers:
            # we are now ready, if the user requested it call the
            # onReady handler
            if self._on_ready:
                # oneshot call
                self._on_ready(self)
                self._on_ready = None
                self._rr_list = self._workers.keys()

    def select(self):
        "select the least loaded not"
        assert len(self._workers) != 0
        if len(self._rr_list) == 0:
            # sort be least loaded
            self._rr_list = sorted(
                self._workers.values(),
                key=lambda k: k['score'])

        # get the least loaded and most responsive.
        r = self._rr_list.pop(0)
        return r['peerId']

    def object_ready(self, client, msg):
        logging.debug("control object_ready called")
        # remote object is now ready to be used by controller
        m = json.loads(msg.payload.decode())
        oid = m['oid']
        if oid in self._objRegistry:
            # call success function of the deploy method
            record = self._objRegistry[oid]

            # inform user that his object is ready for use.
            logging.info("object %s (%s) is ready for use" % (
                record.obj.__class__.__name__, record.obj.oid
            ))

            # when ready call ready()
            objCtrl = RoiSurrogate(record.obj, self)
            record.ready(objCtrl)
        else:
            r = ",".join(self._objRegistry.keys())
            logging.warning("ready message for unknown object %s not in %s" % (oid,r))
             

    def on_connect(self, client, userdata, flags, rc):
        RoiPeer.on_connect(self, client, userdata, flags, rc)

        # if so subscribe to get load factors of the other peers
        # this will influence the round robin scheduling.
        self.subscribe(ROI_TOPIC_LOAD, self.load_handler)
        self.subscribe(ROI_TOPIC_EXCEPTION, self.on_exc)
        self.subscribe(ROI_TOPIC_OBJECT_READY, self.object_ready)

        # signal that this object is ready for use

    def on_exc(self, client, msg):
        jobj = json.loads(msg.payload.decode())
        logging.error("%s: %s" % (jobj['reason'],jobj['stacktrace']))

    def __init__(self, **kwargs):
        RoiPeer.__init__(self, **kwargs)
        self._workers = {}
        self._min_workers = kwargs.get('min_workers', MIN_WORKERS)
        self._rr_list = []
        self._objRegistry = {}



    def deploy(self, obj, ready, failure, **kwargs):
        """
        class SomeObject:
            def remote_method1(self):
                ...

        def onFailure(oid, method, error, stacktrace):
            ...

        def onReady(objCtrl):
            # call and capture response
            objCtrl.remote_method1( ..., reply=handle_function_result )

            # call and forget
            objCtrl.remote_method1( ... )


        RoiControlPeer.deploy( some_object, onReady, onFailure )
        """
        obj.oid = kwargs.get('oid', uuid.uuid1().hex)

        record = RoiRecord(obj, ready, failure)
        # register object with this control node
        self._objRegistry[obj.oid] = record

        # pick a worker node
        worker_host = self.select()

        # all method error exception are routed to the failure 
        # callback
        errTopic = ROI_TOPIC_METHOD_FAIL % {'oid': obj.oid}
        def route_failure( client, msg ):
            jobj = json.loads(msg.payload.decode())
            method =  jobj['method']
            reason = jobj['reason']
            stacktrace = jobj['stacktrace']
            failure(method, reason, stacktrace)
        self.subscribe(errTopic, route_failure)  

        # deploy object to node
        topic = ROI_TOPIC_REGISTER % {
            'peerId': worker_host
        }
          

        logging.debug("control: publishing to %s a pickled object" % topic)
        self._mqttc.publish(topic, payload=pickle.dumps(obj))

        # the worker process will, after registering call ROI_TOPIC_OBJECT_READY
        # which we will receive and then call ready(RoiSurrogate(obj, self))
        # so the end user can use his object remotely.


class SysloadTimer(threading.Thread):
    def __init__(self, peerId, mqttc):
        threading.Thread.__init__(self)
        self.daemon = True
        self.mqttc = mqttc
        self.peerId = peerId
        self.stopEvent = threading.Event()

    def stop(self):
        self.stopEvent.set()

    def run(self):
        while not self.stopEvent.isSet():
            try:
                self.mqttc.publish(ROI_TOPIC_LOAD, payload=json.dumps({
                    'loadFactor': os.getloadavg(),
                    'peerId': self.peerId
                }))
            except OSError:
                pass  # load average unavailable.
            self.stopEvent.wait(SYS_LOAD_SAMPLE_INTERVAL)


class RoiWorkerPeer(RoiPeer):
    """ Provides a serve to a control process that executes remote methods
        on objects the control process has registered.
    """

    def bind_ready(self, remote, n):
        "bind a topic we subscribed to, to a method of the object"
        def handler(client, msg):
            # inbound rpc or send and forget message
            exc_reason = ""
            try:
                exc_reason = "json loads failed"
                req = json.loads(msg.payload.decode())
                args = req['args']
                kwargs = req['kwargs']

                exc_reason = "method exception"

                # call method requested by remote and capture result
                reply = getattr(remote, n)(*args, **kwargs)
                if 'resp_topic' in req:
                    exc_reason = "publish failed"
                    # route result of function call to remote host if
                    # requested.
                    client.publish(
                        req['resp_topic'],
                        payload=json.dumps(reply))

            except BaseException:
                # json parse error
                client.publish(ROI_TOPIC_METHOD_FAIL % {
                    'oid': remote.oid
                }, payload=json.dumps({
                    'method': n,
                    'reason': exc_reason,
                    'stacktrace': traceback.format_exc()
                }))
           # end handler for inbound calls.

        # subscribe to response
        topic = ROI_TOPIC_METHOD % {
            'oid': remote.oid,
            'method': n
        }
        self.subscribe(topic, handler)
        return topic

    def register_object_handler(self, client, msg):
        # register an object, subscribe to topics based on the
        # oid/method name
        try:
            remote = pickle.loads(msg.payload)
        except BaseException:
            self._mqttc.publish(ROI_TOPIC_EXCEPTION, payload=json.dumps({
                "reason": "pickle failed",
                "stacktrace": traceback.format_exc()
            }))
            return

        # subscribe to topics associated with all callable methods
        # save topics in a list for easy deletion when/if the object
        # is unregistered.
        topics = []
        objmethods = dir(object())
        for n in dir(remote):
            if n not in objmethods and callable(getattr(remote, n)):
                # bind method to a mqtt topic, handles communication
                # within a nested function.
                topic = self.bind_ready(remote, n)
                topics.append(topic)
        un_register_topic = ROI_TOPIC_UNREGISTER % {
            'oid': remote.oid
        }

        def on_unregister(client, msg):
            # handle inbound unregister request
            for topic in topics:
                self.unsubscribe(topic)
            self.unsubscribe(un_register_topic)

        # allow user to ubsubscribe to all topics, thus removing all
        # references to remote.
        self.subscribe(un_register_topic, on_unregister)

        # alert control that this object is ready for use.
        logging.debug("worker says object %s is ready to be used" % remote.oid)
        self._mqttc.publish(ROI_TOPIC_OBJECT_READY, payload=json.dumps({
            "oid": remote.oid
        }))

    def on_connect(self, client, userdata, flags, rc):
        RoiPeer.on_connect(self, client, userdata, flags, rc)

        # Handle an incoming request to register on object with this
        # peer
        self.subscribe(ROI_TOPIC_REGISTER % {
            'peerId': self._peerId
        }, self.register_object_handler)
        # start polling for system load
        self.timer.start()

    def __del__(self):
        self.timer.stop()
        self._mqttc.loop_stop()

    def __init__(self, **kwargs):
        RoiPeer.__init__(self, **kwargs)
        self.timer = SysloadTimer(self._peerId, self._mqttc)


class test_object:
    def add(self, n):
        return n + 1
    def die(self):
        raise RuntimeError("test error handling")


def unittest():
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s %(message)s",
        level=logging.DEBUG)


    t = test_object()

    def onSysReady(c):
        logging.debug("All worker nodes have checked in")

        def objectDeployed(objCtrl):
            logging.debug("object deployed")

            def result(n):
                logging.debug("result received")
                print("result called n = " + str(n))
            logging.debug("remote procedure call add 1 to 20")
            objCtrl.add(20, reply=result)
            objCtrl.die()

        def failure(method, reason, stacktrace):
            print("method %s failed %s: %s" % (
                method, reason, stacktrace
            ))

        logging.debug("deploy object")
        c.deploy(t, objectDeployed, failure)

    w1 = RoiWorkerPeer(peerId="node1")
    w2 = RoiWorkerPeer(peerId="node2")
    w3 = RoiWorkerPeer(peerId="node3")
    c = RoiControlPeer(peerId="control", onReady=onSysReady)

    logging.debug("Waiting for control to connect")
    c.connected.wait()
    logging.debug("Control to connected")

    time.sleep(60)


if __name__ == '__main__':
    unittest()

