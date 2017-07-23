Simple library that allows for a control object to deploy arbitrary objects to worker machines within a cluster. It supports a simple load balancer and hides the remote procedure calls to the end user.

Internally it uses the MQTT client for transport and messaging. It was designed for python3. It currently uses asynchronous calls to prevent making blocking calls.


Example network

              +-----<worker1>  
              |
<control>+----+
              |
              +-----<worker2>

Initialization:

Control machine                                 
    import roi

    # called when all workers have checked in and cluster 
    # ready for use  
    def onClusterReady( control ):
        … 
                                                   
    roi.RoiControlPeer(                                          
        peerId="control", 
        onReady=onClusterReady
    )    


Worker Machines 1 … N
    import roi

    # self contained internal thread no more work than
    # this needs to be done.
    roi.RoiWorkerPeer(peerId="node1") # defaults to host name
    
    


















Example Usage:

# Object you wish to distribute 
class MyObject:
    def add(self, n):
        return n + 1
    def die(self):
        raise RuntimeError(‘test exception handling’)

def onClusterReady( control ):
    # example of user wishing to send an instance of MyObject
    # to a cluster then calling a method and returning the 
    # result.

    def objectDeployed(objCtrl):
         # Instance of MyObject has been deployed to a worker
         # node and is now ready for use.
        
         def result(n):
             # capture result of remote method call
             print("result called n = " + str(n))
            
         objCtrl.add(20, reply=result)

    def failure(method, reason, stacktrace):
         … # exception handling for all method calls.

    c.deploy(t, objectDeployed, failure)

