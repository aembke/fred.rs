Minikube
========

Running Redis inside minikube can be difficult. This document will cover some of the issues associated with the following common use case:

* You're running a Redis **cluster** or Redis sentinel inside minikube. If you're just using a centralized server nothing in this document will likely apply to you.
* You're running your application outside of minikube, but it needs to connect to Redis inside minikube. This is often how people use minikube while actively developing their app layer.
* You're using either the virtualbox driver or the docker driver without host networking for minikube. In other words, minikube is running its own virtual network.
* You have limited access to the minikube network from outside the minikube cluster. This may come in of the form of a load balancer or reverse proxy where your app layer connects to this proxy instead of anything directly inside the minikube cluster.

Before getting too far into this it's necessary to understand how Redis cluster and Redis sentinel work, and why they make this difficult.

### Redis Cluster

The Redis cluster features work by sharding data across multiple Redis servers. 

Keys are assigned a hash slot where each hash slot is a 16-bit integer that comes from a CRC16 of the key or key [hash tag](https://redis.io/topics/cluster-spec#keys-hash-tags). Each primary/main node in the cluster is responsible for a range of hash slots. Any node can cover multiple unique hash slots ranges, but all possible hash slots in the range `0-16383` must be covered. A hash slot can only belong to one node in the cluster.

The key hashing logic is implemented [here](https://docs.rs/redis-protocol/latest/redis_protocol/fn.redis_keyslot.html).

The state of the cluster hash slot assignments can be seen via the `CLUSTER NODES` command. Here is a sample output of `CLUSTER NODES` from a local cluster started via the `create-cluster` script that comes with Redis.

```
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460
```

In this example there are 3 primary nodes and 3 replicas. Each primary has one replica. The 16383 hash slots are spread evenly across the 3 primary nodes.

* `127.0.0.1:30001` covers the hash slot range 0-5460
* `127.0.0.1:30002` covers the hash slot range 5461-10922
* `127.0.0.1:30003` covers the hash slot range 10923-16383

In order for any Redis client to support the full command set with any possible key it needs to connect to 3 nodes. There are several ways this can be done, but generally speaking most clients will maintain at least one connection to each primary node in the cluster.

There are a couple other things to note about Redis cluster usage:

* Cluster state can change. Replicas can be promoted to primary nodes, and vice versa when servers shut down.
* Hash slots can move between nodes.
* Nodes can be added or removed from the cluster, which often comes with a hash slot rebalancing operation.

This doc will not cover how this happens because it's often not necessary to know that for minikube usage. However there are a few important things to remember with Redis clusters and minikube:

* Clients often need to be aware of the stata of all cluster nodes in order to know which nodes need a connection. Generally speaking clients connect to each primary node. 
* The `CLUSTER NODES` response contains IP addresses for all of the nodes.
* The client has to use the `CLUSTER NODES` response to initiate connections to other nodes.

The general idea is that at any time a client can send the `CLUSTER NODES` command to _any_ node, and that node will return the state of the entire cluster. Clients then use that response to initiate connections to all the other nodes.

This is how fred works. When initiating a connection to a cluster it does the following:

1. Read the client's `ServerConfig::Cluster` to find the list of known hosts/ports provided by the caller.
2. Try to connect to each of them. If none can accept a connection then return an error.
3. Once a connection is established run the `CLUSTER NODES` command. 
4. Parse the `CLUSTER NODES` response to read the IP address and port of each node in the cluster.
5. Initiate connections to each of the main/primary nodes in the cluster.
6. Start accepting commands from callers.

### Redis Sentinel

When using a Redis server behind a Sentinel layer the process is somewhat similar to a cluster. Redis Sentinel works by adding a management layer in front of the Redis server that tracks the health of the Redis server. If the Redis server dies unexpectedly the sentinel can fail over to a replica. Clients connect to the sentinel first in order to learn about how they should connect to backing Redis server. Typically these sentinel nodes are deployed on different machines or containers.

When connecting to a sentinel node the client does the following:

1. Read the client's `ServerConfig::Sentinel` to find the list of known hosts/ports for the sentinel nodes provided by the caller.
2. Try to connect to each of the sentinel nodes. If none can accept connections then fail over.
3. Run the `SENTINEL get-master-addr-by-name` command on the sentinel node. 
4. Parse the response to find the IP address / port for the main Redis server. The sentinel will change this if it needs to fail over to a replica, etc.
5. Connect to the IP address / port from the previous step. Return an error if a connection cannot be established.
6. Start accepting commands from callers.

### Minikube Considerations

**The key thing to take away from the previous sections is that for both Redis cluster and Redis Sentinel the servers will redirect clients by returning IP addresses and ports for other server instances.**

This implementation decision by Redis assumes a few things about your client setup:

* The client has access to the same network as the server. An IP address X.X.X.X maps to the same thing from the client's perspective as it does from the server's perspective.
* The client's DNS settings are similar to the server's DNS settings. This really only matters with TLS, which is not covered in this doc.

When using minikube neither of the above assumptions are true due to how minikube runs its own private network.

You may connect to minikube via a 10.0.0.0/24 address, but inside minikube everything uses a 172.168.0.0/16 address, or something other than the 10/24 range used outside the minikube cluster.

This is problematic for a few reasons. Consider the Redis Cluster example:

1. You provide the address `10.0.0.1` to fred as the IP address for one of the cluster nodes. Maybe the other nodes are visible outside minikube, maybe not. It doesn't really matter though in this example.
2. Fred connects to `10.0.0.1` successfully and runs `CLUSTER NODES`. 
3. When parsing the response it sees `172.168.0.0/16` addresses instead of `10.0.0.0/24` addresses, since that's the IP range used inside minikube.
4. Fred tries to connect to one of those IP addresses, let's say `172.168.0.1`, but it fails with a networking error (typically `HOSTUNREACHABLE`). 

This happens because the network outside minikube has no idea how to access the IP addresses inside minikube. The minikube network is private and might as well be in another data center. Your host machine has no idea how to send traffic to anything in that `172.168.0.0/16` range directly without more information.

This is why many times people run reverse proxies (such as Caddy) with minikube. One of the goals of services like Caddy is to handle this exact situation.

Folks typically discover this since their HTTP app can't be accessed inside minikube. Then they find out about Caddy, and they look for something similar to use for Redis (or other data layer services).

However, Redis is uniquely ill-suited for this kind of usage. With HTTP the story is pretty simple. The client and reverse proxy can rely on DNS features to identify servers and re-route traffic. With Redis the problem is more complicated because Redis returns IP addresses, not hostnames, so we cannot leverage DNS rules to re-route traffic.

This is why Redis Cluster or Redis Sentinel + Minikube is complicated.

### Solutions

There are not currently many great ways to solve this. 

As you probably gathered, using a centralized Redis server with minikube avoids all of these problems. You only need to poke a small hole into the minikube network, and the client can simply connect to the one Redis server via that exposed address/port from the host. Since there's only one node the server will never redirect the client elsewhere. **This is almost always the easiest option.**

If your main goal with using minikube is to facilitate easy local development this option often works fine. A common setup is to use a centralized server in minikube, but clusters in "real" environments like staging and production. Callers then just need to write code such that they can configure the client easily via argv, environment variables, or configuration files. Then all you need to do is use a different configuration in upper environments to switch to a cluster.

However, if you need access to a cluster in order to use `CLUSTER *` commands, etc, then another option is to run minikube in a way where each container in the cluster is visible to the host machine. 

If you're using the docker driver you have a few other options as well. Docker networking is easier to manage than virtualbox networking. It is also possible to "dockerize" your local build or run script so that it runs via `docker` or `docker-compose`. If you do this you can supply the minikube docker network as an argument to your `docker run` command, which will run your container inside the same network as the Redis server. This is what Fred does in its CI configuration since CircleCI runs some of the Redis servers on a remote docker engine.

Aside from working around the networking restrictions unfortunately I'm not sure of a great solution here. It's possible that there are services out there to deal with this issue, but I'm not aware of them. If you know of a better solution here please file an issue to update this document and I'd be happy to add it.

