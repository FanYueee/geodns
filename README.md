# GeoDNS servers

This is the DNS server powering the [NTP Pool](http://www.pool.ntp.org/) system
and other similar services.

[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/7022/badge)](https://bestpractices.coreinfrastructure.org/projects/7022)

## Questions or suggestions?

For bug reports or feature requests, please create [an
issue](https://github.com/abh/geodns/issues). For questions or
discussion, you can post to the [GeoDNS
category](https://community.ntppool.org/c/geodns) on the NTP Pool
forum.

## Installation

Release builds are available in a yum repository at
`https://pkgs.ntppool.org/yum/` and apt (debian, ubuntu) packages at
`https://pkgs.ntppool.org/apt/`.

### From source

If you don't have Go installed the easiest way to build geodns from source is to
download and install Go from `https://golang.org/dl/`.

GeoDNS generally requires a recent version of Go (one of the last few major versions)

```sh
git clone https://github.com/abh/geodns.git
cd geodns
go build
./geodns -h
```

You can also build with [goreleaser](https://github.com/goreleaser/goreleaser).

## Sample configuration

There's a sample configuration file in `dns/example.com.json`. This is currently
derived from the `test.example.com` data used for unit tests and not an example
of a "best practices" configuration.

For testing there's also a bigger test file at:

```sh
mkdir -p dns
curl -o dns/test.ntppool.org.json http://tmp.askask.com/2012/08/dns/ntppool.org.json.big
```

## Run it

After building the server you can run it with:

`./geodns -log -interface 127.1 -port 5053`

To test the responses run

`dig -t a test.example.com @127.1 -p 5053`

or

`dig -t ptr 2.1.168.192.IN-ADDR.ARPA. @127.1 -p 5053`

or more simply put

`dig -x 192.168.1.2 @127.1 -p 5053`

The binary can be moved to /usr/local/bin, /opt/geodns/ or wherever you find appropriate.

### Configuration

See the [sample configuration file](https://github.com/abh/geodns/blob/main/dns/geodns.conf.sample).

Notable command line parameters (and their defaults)

* -config="./dns/"

Directory of zone files (and configuration named `geodns.conf`), or the path to
the configuration file itself. With a file path, its parent directory holds the
synced Zone JSON files. `-configfile` remains available for directory-based use.

* -checkconfig=false

Check configuration file, parse zone files and exit

* -interface="*"

Comma separated IPs to listen on for DNS requests.

* -port="53"

Port number for DNS requests (UDP and TCP)

DNS addresses and the default port can also be set as `[dns] listen` (a
comma-separated list, including IPv6) and `port` in `geodns.conf`. Explicit
`-interface` and `-port` flags override them. Listener and cluster settings take
effect at startup; Zone file updates reload while running.

* -http=":8053"

Listen address for HTTP interface. Specify as `127.0.0.1:8053` to only listen on
localhost.

* -identifier=""

Identifier for this instance (hostname, pop name or similar).

It can also be a comma separated list of identifiers where the first is the "server id"
and subsequent ones are "group names", for example region of the server, name of anycast
cluster the server is part of, etc. This is used in (future) reporting/statistics features.

* -log=false

Enable to get lots of extra logging, only useful for testing and debugging. Absolutely not
recommended in production unless you get very few queries (less than 1-200/second).

* -cpus=4

Maximum number of CPUs to use. Set to 0 to match the number of CPUs
available on the system (also the default).

## Logging

GeoDNS supports query logging to JSON or Avro files (see the sample configuration file
for options).

## Synchronizing zones across servers

One server publishes its zone JSON files to followers over authenticated
WebSocket connections. Each follower connects outward to the master and answers
DNS queries independently. Changes are pushed when the master's zone directory
changes; the follower validates and reloads them before acknowledging a revision.
Omit `[sync]` to keep running as a standalone server.

On the master, add the `[sync]` section shown in
[`dns/geodns.master.conf.sample`](dns/geodns.master.conf.sample) to its local
`geodns.conf`. Set `mode = master` and a shared, random `token`. The stream is
`GET /api/v1/stream` on the existing `-http` listener; start that listener on a
private address or behind an HTTPS reverse proxy that supports WebSocket upgrades.
All sync endpoints require `Authorization: Bearer <token>`, including when HTTP
Basic Auth is enabled for other HTTP endpoints. `GET /api/v1/nodes` returns
connected state, last heartbeat, applied revision and last error for each node.
These fields show sync connection and application status, not a DNS query probe.

On each follower, use a dedicated `-config` directory with its own local
`geodns.conf` based on
[`dns/geodns.follower.conf.sample`](dns/geodns.follower.conf.sample). Set
`mode = follower`, a unique `id`, `url` to the master's HTTP(S) origin (without
the API path), and the same `token`. The follower connects at startup and
automatically reconnects after a disconnect. The master sends a WebSocket ping
every five seconds and marks an unresponsive node offline after 15 seconds.
Connection, disconnection, heartbeat timeout, and failed or successful zone
application are written to the normal process log on both sides (stderr by
default, or the file selected by `-logfile`). Disconnection records include the
node ID, remote address, reason, and last contact time. A successful sync removes
local zone JSON files absent from the master; do not put hand-maintained zones in a
follower's `-config` directory. If the master is unavailable, the node keeps
answering with its last applied zones.

The existing `mode = master` runs inside the GeoDNS process. To run the same
single-master behavior without a DNS listener, build the controller with
`go build -o geodns-controller ./cmd/geodns-controller` and start it with
`-mode single`, `-config` pointing to the zone directory,
`-configfile` pointing to a config based on
[`dns/geodns.controller.conf.sample`](dns/geodns.controller.conf.sample), and
`-http` on the private network.
Its `-logfile` and `-checkconfig` flags work like GeoDNS's. Only one master should
serve a given set of followers in this mode.

### One-process HA with embedded etcd

For the simplest HA deployment, use
[`dns/geodns.cluster.conf.sample`](dns/geodns.cluster.conf.sample). The official
etcd server is embedded in `geodns`: one binary, one configuration file, and one
process per host run DNS, the shared store, controller election, WebSocket
updates, and node heartbeats. No separate etcd installation or `etcd.yml` is
needed. Every node serves DNS and is eligible to become the GeoDNS controller.
The original standalone mode remains the default when `[cluster]` is absent or
`enabled = false`; disabled cluster fields are ignored.

On three hosts with reachable private/WireGuard IPs, create a dedicated
directory such as `/srv/geodns/node` containing `geodns.conf`. For the first host:

```ini
[dns]
listen = 10.80.0.11

[cluster]
enabled = true
id = pop-1
weight = 300
address = 10.80.0.11
token = replace-with-the-same-long-random-secret
members = pop-1=10.80.0.11,pop-2=10.80.0.12,pop-3=10.80.0.13
zone-directory = source-zones
```

On the second and third hosts, change `id`, `address`, and `[dns] listen` to that
host's values; keep `members` and `token` identical and omit `zone-directory`.
Set `weight` to `200` on the second host and `100` on the third to prefer
`pop-1`, then `pop-2`, then `pop-3` as controller. Higher weights take precedence:
if `pop-1` goes offline, `pop-2` takes over; when `pop-1` rejoins the election,
the current controller closes its sync streams and yields automatically. Nodes
reconnect to the new controller and keep answering DNS from their local Zones.
Weights are nonnegative integers and default to `0`. Equal weights keep the
existing controller, so deployments without weights retain their previous
election behavior. Weight changes require restarting that node; Zone file edits
remain live. This controls the **GeoDNS master**, not etcd's internal Raft leader
or DNS record weights, and still requires etcd quorum. For external etcd HA or
the separate controller binary, set `weight` in `[controller]` instead.
Upgrade all controller candidates before relying on priority: older binaries
do not participate in priority handoff. Candidate registration, election weights,
handoff targets, and leadership endings are recorded in the logs.

`address` binds embedded etcd and the sync API to this node's private IP. DNS
`listen` may instead list public addresses, e.g.
`23.160.172.53,2602:f37b:53::53`. Put initial Zone JSON files in the first host's
`source-zones/` directory beside `geodns.conf`, then start each host:

```sh
geodns -config /srv/geodns/node/geodns.conf
```

Initial Zones are imported automatically and later source file edits are
validated and pushed live. Do not manually edit the received JSON files beside
`geodns.conf`. The default local etcd data directory is `etcd-data/` beside the
configuration file; preserve it across upgrades and restarts. Override it with
`[cluster] data-directory` to use another persistent volume. Cluster `name`
defaults to `geodns` and must match on every host. Optional `client-port`,
`peer-port`, and `sync-port` default to 2379, 2380, and 8053; all members must
share these port settings. Do not combine `[cluster]` with `[sync]` or
`[controller]`; use the external mode below when you already run etcd.
Embedded mode creates a new store and does not migrate external etcd data.
Keep an existing deployment on external mode until its latest Zone files and
backups are prepared for migration; an external server cannot listen on the
same addresses and ports as the embedded server.

To add another host, use the same config with a new `id`, `address`, and DNS
`listen`, omit both `members` and `zone-directory`, and set:

```ini
join = 10.80.0.11,10.80.0.12,10.80.0.13
```

Start it with the same command. GeoDNS registers its embedded etcd as a learner,
starts replication, and promotes it to a voting member once caught up; existing
configs do not change. Pending joins and promotion retry automatically. Keep
the join config and persistent data on restarts. Add members one at a time;
three or five voters are the usual HA layouts. A four-voter cluster requires
three online members and tolerates one failure, just like a three-voter cluster.

To inspect registered members and the elected GeoDNS controller, run on a live
host (this command does not start a second embedded server):

```sh
geodns -config /srv/geodns/node/geodns.conf -cluster-status
```

To remove a host permanently, stop its GeoDNS process, then run on another live
host while the cluster still has quorum:

```sh
geodns -config /srv/geodns/node/geodns.conf -cluster-remove pop-4
```

The remove flag also accepts the hexadecimal `MEMBER-ID` shown by
`-cluster-status`, including learners whose startup failed before they acquired
a name. Stop any joining process before removing its pending member.

Stopping a process only stops its election candidacy; permanently removing its
etcd voter requires this command so the quorum size is updated. Preserve the
removed member's old data for recovery and do not restart it with an empty data
directory and its old `members` list. Joining again is a fresh member operation
with a new data directory and `join` configuration.

Embedded etcd clients discover current voting members automatically. Startup and
membership events, promotion failures, and disconnects are logged by GeoDNS.
TCP 2379, 2380, and 8053 must be reachable between the nodes, while DNS uses
TCP/UDP 53 by default. Restrict the embedded etcd and sync ports to the private
network: the shared token authenticates GeoDNS sync, **not** etcd client or peer
requests. Embedded mode uses HTTP over the private tunnel; use the external etcd
mode for authenticated TLS configurations. A majority of etcd voters must be
online for election and publication; nodes with cached Zones can continue
answering DNS during loss of quorum. Back up persistent etcd data using etcd's
snapshot tools before maintenance. Automatic compaction retains one hour of
store history.

### HA with external etcd

For automatic controller failover with a separately managed store, deploy three etcd members on separate hosts
reachable over a private network. On each host, copy
[`dns/etcd.ha.yml.sample`](dns/etcd.ha.yml.sample) to `/etc/geodns/etcd.yml`.
Change the member name and its local IPs on each host, but keep the complete
three-member `initial-cluster` list identical. Give etcd a persistent, writable
data directory and start it with `etcd --config-file /etc/geodns/etcd.yml`.

When each host also serves DNS, build only `geodns` and put a copy of
[`dns/geodns.ha-node.conf.sample`](dns/geodns.ha-node.conf.sample) in each
node's dedicated `-config` directory as `geodns.conf`. Use a unique `[sync] id`
and the host's private `[controller] listen` address; keep `etcd-endpoints`,
`etcd-prefix`, and the token identical. Every GeoDNS node serves DNS and is
eligible for controller election, including nodes added later. No GeoDNS URL
list is needed: each candidate advertises its own HTTP origin in its leased
etcd election record, and nodes discover the elected candidate when connecting
or reconnecting. Use a private, reachable address for `listen`. If listening on
a wildcard or using an HTTPS reverse proxy, set `[controller] advertise` to
the reachable HTTP(S) origin instead.

Before the first startup, put the initial Zone JSON files in a **separate**
source directory on one node and set `[controller] zone-directory` there.
For example, `zone-directory = source-zones` refers to a `source-zones/`
subdirectory beside that node's `geodns.conf`; other nodes omit this setting.
Start one GeoDNS process per host:

```sh
geodns -config /srv/geodns/node
```

This process answers DNS, participates in controller election, and connects to
the elected leader to receive zones, including when it is the leader. The
private HTTP listener uses `[controller] listen`; `-http` overrides it. Keep
the node's `-config` directory for synced zones and its local `geodns.conf`.
The node with `zone-directory` automatically imports its files only when the
cluster has no published snapshot. The import retries if etcd is not yet
available. Concurrent initial imports are serialized; the first valid snapshot
wins. Empty or invalid source directories are rejected and logged. Once a
snapshot exists, restarts use cluster data even if the local source is stale or
missing. No initial `-publish` command is required.

For later changes, save the Zone JSON files in that node's `source-zones/`
directory. The running HA node watches this directory, waits 500 ms for a burst
of edits to settle, validates the complete snapshot, and automatically publishes
it to etcd. The elected controller then pushes the update to every connected
node, which reloads DNS without restarting. This works even when the editing
node is not the elected controller. Invalid files or unavailable etcd are logged
and retried; the last published zones remain active. Local scans every five
seconds recover missed file events; nodes still receive updates by WebSocket,
not by polling snapshots. Adding, replacing, and removing individual Zone files
are supported. An empty source directory is not automatically published; use
`-publish` explicitly if you intend to remove all zones.

Keep `zone-directory` on one editing node to avoid competing local sources.
Its startup files establish a local change baseline: unchanged stale files never
overwrite existing cluster data, including after another host publishes.
Edits made while GeoDNS is stopped are not automatically published on restart
when the cluster already contains zones; use the explicit command in that case.
The same binary and config can still publish manually from a host reaching etcd:

```sh
geodns -config /srv/geodns/node -publish
```

The publish command validates the files,
stores a complete snapshot in etcd, and atomically makes that snapshot current.
Only the controller holding the election lease accepts node connections;
others return HTTP 503. Run etcd and GeoDNS as separate long-lived services
in production.

To add a node, copy the HA node config, change its `id` and `listen`, provide
the same token and etcd settings, and start `geodns`. It joins the election and
receives the current zones without changing or restarting existing nodes.
To remove a GeoDNS node, stop it; its election record disappears on resignation
or lease expiry. Disconnections are logged and known offline status is retained
for monitoring. This changes the GeoDNS node set, not etcd membership; the shared
three-member etcd quorum can serve any number of GeoDNS election candidates.
Adding or removing an actual etcd member still requires etcd's member management.

For independent controller processes instead, copy
[`dns/geodns.controller.ha.conf.sample`](dns/geodns.controller.ha.conf.sample)
to `/etc/geodns/controller.conf` on each controller host. Change its `id` and
`listen` for that host; keep endpoints, prefix, and token identical. Start it
with `geodns-controller -configfile /etc/geodns/controller.conf` and publish
with the same command plus `-publish`. In that layout, use
[`dns/geodns.follower.ha.conf.sample`](dns/geodns.follower.ha.conf.sample) on
each separate PoP. The node tries the next controller on disconnect and keeps
answering with its last applied zones while no leader is available.
The controller's existing `-mode`, `-id`, `-http`, `-config`, `-etcd`, and
`-etcd-*` flags remain available and override the corresponding config entries
when explicitly provided. Relative paths in `[controller]` resolve from the
controller config file's directory.
Existing `[sync] urls` lists in HA node configs continue to select the explicit
connection path. To migrate an existing cluster to discovery, first upgrade all
candidates and ensure each advertises a reachable address, then remove `urls`.

The leader persists known node status every five seconds; a new leader
initially marks those nodes offline until they reconnect. An abrupt controller
failure requires its ten-second etcd lease to expire, followed by node
reconnection. The HA mode requires an etcd quorum; `[controller]` also accepts
`etcd-user`, `etcd-password-file`, `etcd-ca`, `etcd-cert`, and `etcd-key` for
authenticated TLS deployments. Restrict controller and etcd ports to the
private network. etcd should have persistent storage, backups and compaction
configured. This feature does not configure WireGuard, probe DNS service
availability, or control BGP announcements.
A single WireGuard hub remains a separate failure point even with three etcd
members; provide redundant tunnel paths if controller failover must survive a
hub outage.

The sync token and role settings are local configuration; changing them takes
a restart. Keep configuration files containing tokens private (for example,
mode `0600`). GeoIP databases, health status files, log paths, and other
node-specific settings are managed separately. Use `-checkconfig` to validate
local role settings and zone files before starting a node.

## Prometheus metrics

`/metrics` on the http port provides a number of metrics in Prometheus format.

### Runtime status page, Websocket metrics & StatHat integration

The runtime status page, websocket feature and StatHat integration have
been replaced with Prometheus metrics.

## Country and continent lookups

See zone targeting options below.

## Weighted records

Most records can have a 'weight' assigned. If any records of a particular type
for a particular name have a weight, the system will return `max_hosts` records
(default 2).

If the weight for all records is 0, all matching records will be returned. The
weight for a label can be any integer as long as the weights for a label and record
type is less than 2 billion.

As an example, if you configure

    10.0.0.1, weight 10
    10.0.0.2, weight 20
    10.0.0.3, weight 30
    10.0.0.4, weight 40

with `max_hosts` 2 then .4 will be returned about 4 times more often than .1.

## Configuration file

The geodns.conf file allows you to specify a specific directory for the GeoIP
data files and other options. See the `geodns.conf.sample` file for example
configuration.

The global configuration file is not reloaded at runtime.

Most of the configuration is "per zone" and done in the zone .json files.
The zone configuration files are automatically reloaded when they change.

## Zone format

In the zone configuration file the whole zone is a big hash (associative array).
At the top level you can (optionally) set some options with the keys serial,
ttl and max_hosts.

The actual zone data (dns records) is in a hash under the key "data". The keys
in the hash are hostnames and the value for each hostname is yet another hash
where the keys are record types (lowercase) and the values an array of records.

For example to setup an MX record at the zone apex and then have a different
A record for users in Europe than anywhere else, use:

    {
        "serial": 1,
        "data": {
            "": {
                "ns": [ "ns.example.net", "ns2.example.net" ],
                "txt": "Example zone",
                "spf": [ { "spf": "v=spf1 ~all", "weight": 1 } ],
                "mx": { "mx": "mail.example.com", "preference": 10 }
            },
            "mail": { "a": [ ["192.168.0.1", 100], ["192.168.10.1", 50] ] },
            "mail.europe": { "a": [ ["192.168.255.1", 0] ] },
            "smtp": { "alias": "mail" }
        }
    }

The configuration files are automatically reloaded when they're updated. If a file
can't be read (invalid JSON, for example) the previous configuration for that zone
will be kept.

## Zone options

* serial

GeoDNS doesn't support zone transfers (AXFR), so the serial number is only used
for debugging and monitoring. The default is the 'last modified' timestamp of
the zone file.

* ttl

Set the default TTL for the zone (default 120).

* targeting

* max_hosts

* contact

Set the soa 'contact' field (default is "hostmaster.$domain").

## Zone targeting options

@

country
continent

region and regiongroup

## Supported record types

Each label has a hash (object/associative array) of record data, the keys are the type.
The supported types and their options are listed below.

Adding support for more record types is relatively straight forward, please open a
ticket in the issue tracker with what you are missing.

### A

Each record has the format of a short array with the first element being the
IP address and the second the weight.

    [ [ "192.168.0.1", 10], ["192.168.2.1", 5] ]

See above for how the weights work.

### AAAA

Same format as A records (except the record type is "aaaa").

### Alias

Internally resolved cname, of sorts. Only works internally in a zone.

    "foo"

### CNAME

    "target.example.com."
    "www"

The target will have the current zone name appended if it's not a FQDN (since v2.2.0).

### MX

MX records support a `weight` similar to A records to indicate how often the particular
record should be returned.

The `preference` is the MX record preference returned to the client.

    { "mx": "foo.example.com" }
    { "mx": "foo.example.com", "weight": 100 }
    { "mx": "foo.example.com", "weight": 100, "preference": 10 }

`weight` and `preference` are optional.

### NS

NS records for the label, use it on the top level empty label (`""`) to specify
the nameservers for the domain.

    [ "ns1.example.com", "ns2.example.com" ]

There's an alternate legacy syntax that has space for glue records (IPv4 addresses),
but in GeoDNS the values in the object are ignored so the list syntax above is
recommended.

    { "ns1.example.net.": null, "ns2.example.net.": null }

### TXT

Simple syntax

    "Some text"

Or with weights

    { "txt": "Some text", "weight": 10 }

### SPF

An SPF record is semantically identical to a TXT record with the exception that the label is set to 'spf'. An example of an spf record with weights:

    { "spf": "v=spf1 ~all]", "weight": 1 }

An spf record is typically at the root of a zone, and a label can have an array of SPF records, e.g

      "spf": [ { "spf": "v=spf1 ~all", "weight": 1 } , "spf": "v=spf1 10.0.0.1", "weight": 100]

### SRV

An SRV record has four components: the weight, priority, port and target. The keys for these are "srv_weight", "priority", "target" and "port". Note the difference between srv_weight (the weight key for the SRV qtype) and "weight".

An example srv record definition for the _sip._tcp service:

    "_sip._tcp": {
        "srv": [ { "port": 5060, "srv_weight": 100, "priority": 10, "target": "sipserver.example.com."} ]
    },

Much like MX records, SRV records can have multiple targets, eg:

    "_http._tcp": {
        "srv": [
            { "port": 80, "srv_weight": 10, "priority": 10, "target": "www.example.com."},
            { "port": 8080, "srv_weight": 10, "priority": 20, "target": "www2.example.com."}
        ]
    },

## License and Copyright

This software is Copyright 2012-2015 Ask Bjørn Hansen. For licensing information
please see the file called LICENSE.
