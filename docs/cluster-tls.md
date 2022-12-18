Cluster TLS
===========

Configuring TLS settings for a clustered deployment can be complicated. There are several ways to do this and some vendors or managed services require different configuration options on the client.

## Background

Redis cluster works by sharding the data set among a set of primary nodes. The client and server use a [key distribution model](https://redis.io/docs/reference/cluster-spec/#key-distribution-model) that uses CRC-16 as the hashing function for keys. These hash slots are then distributed across the cluster where each hash slot is owned by at most one primary cluster node.

From the docs:

```
HASH_SLOT = CRC16(key) mod 16384
```

Hash slots can contain multiple keys and these hash slots can move between nodes in order to redistribute the data set across the cluster. Clients are expected to hash the key in each command in order to determine which server should receive the command. If a client sends a command to the wrong cluster node the server will respond with a `MOVED` or `ASK` error that indicates the client should retry the command against a different cluster node. The server will not transparently route the command to a different node for the client.

Clients discover the cluster topology and hash slot mappings via the [CLUSTER SLOTS](https://redis.io/commands/cluster-slots/) or [CLUSTER SHARDS](https://redis.io/commands/cluster-shards/) command. This library uses the `CLUSTER SLOTS` command in order to support versions <7.x, but this may change in the future.

The `CLUSTER SLOTS` command returns the following information (from the docs):

```
> CLUSTER SLOTS
1) 1) (integer) 0
   2) (integer) 5460
   3) 1) "127.0.0.1"
      2) (integer) 30001
      3) "09dbe9720cda62f7865eabc5fd8857c5d2678366"
      4) 1) hostname
         2) "host-1.redis.example.com"
   4) 1) "127.0.0.1"
      2) (integer) 30004
      3) "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"
      4) 1) hostname
         2) "host-2.redis.example.com"
2) 1) (integer) 5461
   2) (integer) 10922
   3) 1) "127.0.0.1"
      2) (integer) 30002
      3) "c9d93d9f2c0c524ff34cc11838c2003d8c29e013"
      4) 1) hostname
         2) "host-3.redis.example.com"
   4) 1) "127.0.0.1"
      2) (integer) 30005
      3) "faadb3eb99009de4ab72ad6b6ed87634c7ee410f"
      4) 1) hostname
         2) "host-4.redis.example.com"
3) 1) (integer) 10923
   2) (integer) 16383
   3) 1) "127.0.0.1"
      2) (integer) 30003
      3) "044ec91f325b7595e76dbcb18cc688b6a5b434a1"
      4) 1) hostname
         2) "host-5.redis.example.com"
   4) 1) "127.0.0.1"
      2) (integer) 30006
      3) "58e6e48d41228013e5d9c1c37c5060693925e97e"
      4) 1) hostname
         2) "host-6.redis.example.com"
```

For the purposes of this document the exact structure here doesn't really matter, but it's worth noting a few things:

* The default response contains IP addresses rather than hostnames.
* The `hostname` metadata in each section is optional and is not included in older versions of Redis. Some managed services expose this information, others do not.
* The hostname or IP address can be `null` in some scenarios. Some server configurations use a reverse proxy or ILB such that clients connect to a single hostname in order to access any cluster node.

## TLS Configurations

During the TLS handshake the client is expected to provide a server name against which to validate the server certificate. This is usually the hostname of the node that the caller is trying to communicate with, but in some scenarios the caller may not know this hostname. In the example provided above (assuming the hostname info is disabled) the client only has access to the IP address for each node, but not the hostname. 

In later versions of Redis there are some optional features to help with this use case, but as of writing this many vendors do not use these options.

Consider the following (somewhat common) example:

* The caller provides "host-1.redis.example.com" as the hostname in the `RedisConfig`.
* The client successfully connects to this node and sends `CLUSTER SLOTS`.
* The server responds with the response example above, but without the `hostname` metadata.
* The client tries to connect to "127.0.0.1", but the server cert has a CN of "host-2.redis.example.com" (and no SAN DNS entries).
* The connection attempt fails during the TLS handshake with a certificate verification error.

There are a few ways to solve this:

* Configure the server certificates so that they support multiple hostnames via a star cert (such as "*.redis.example.com").
* Configure the server certificates with SAN DNS entries for the other node hostnames.
* Use a reverse proxy or ILB (which may or may not do TLS termination) that provides one endpoint for clients to use.
* Expose a mechanism for the client to map IP addresses back to hostnames for TLS certificate verification purposes in a manner similar to a reverse DNS lookup.

The first two options require some level of control over the certificate generation process, which is often a non-starter with managed services. The reverse proxy option is somewhat more common, but may still require some level of control over the network plumbing.

Note: If the server exposes the `hostname` metadata field in the `CLUSTER SLOTS` response this library will use that hostname instead of the IP address for both DNS and TLS certificate verification purposes.

## TLS Hostnames

From the client's perspective there's one primary concern here: how to map an IP address back to a hostname for certificate verification purposes. 

The `TlsHostMapping` enum on the `TlsConfig` struct (contained within the `RedisConfig`) exposes an interface to support this use case. Currently, there are two supported variants:

* A `DefaultHost` option that assumes a single hostname is used for all nodes during the TLS handshake. This usually works well with the reverse proxy use case even if the proxy does not do TLS termination. This works by reusing the hostname of the node that responded to `CLUSTER SLOTS` when connecting to new nodes.
* A `Custom` option that allows the caller to pass in a trait object to override the IP -> hostname lookup process. This may use a reverse DNS lookup, or some hard-coded values, or something more complicated. This option is the most flexible but requires more work from the caller.

Both of these interfaces will only use the provided hostname for certificate verification purposes. These hostnames are **not** used for DNS purposes. The client will still connect to the original IP address.
