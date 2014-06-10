SimpleDynamo
============

Simplified implementation of Amazon Dynamo

A distributed key-value type storage was implemented on the lines of Amazon Dynamo the implementation was done on as an Android app.
The key features implemented:
1) Membership: Like Amazon Dynamo, every node knows other nodes in the system
2) Request Routing: When a node receives a request the requests are forwareded to the appropriate node.
3) Replication: The key-value pairs are replicated to provide availablity in case of Node faliures.
4) Object Versioning: To provide consistency object versioning was implemented to avoid any conflicts.
5) Faliure Handling: In case of node failure, the request is redirected to the replica of the appropriate node. 
6) Node recovery: After a node failure, when the node recovers it requests the missed insertions from it's neighboring nodes. 
