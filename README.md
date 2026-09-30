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

Directory of zone files (and configuration named `geodns.conf`).

* -checkconfig=false

Check configuration file, parse zone files and exit

* -interface="*"

Comma separated IPs to listen on for DNS requests.

* -port="53"

Port number for DNS requests (UDP and TCP)

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

For automatic controller failover, deploy a three-member etcd cluster on separate
failure domains, reachable over the private WireGuard network. Each controller
runs the standalone binary with `-mode ha`, a unique `-id`, the same `-etcd`
endpoint list and `-etcd-prefix`, and a local config with `mode = controller`
plus the shared token. HA controllers do not read local zone files while
serving: etcd stores the published snapshot, and only the controller holding the
election lease accepts node connections. The controller returns HTTP 503 until a
snapshot is published or whenever it is not the ready leader.
For example, start the first etcd member with the following settings, changing
`--name`, its local IP and data directory on the other two members:

```sh
etcd --name controller-1 --data-dir /var/lib/etcd-geodns \
  --listen-client-urls http://10.80.0.11:2379 \
  --advertise-client-urls http://10.80.0.11:2379 \
  --listen-peer-urls http://10.80.0.11:2380 \
  --initial-advertise-peer-urls http://10.80.0.11:2380 \
  --initial-cluster controller-1=http://10.80.0.11:2380,controller-2=http://10.80.0.12:2380,controller-3=http://10.80.0.13:2380 \
  --initial-cluster-token geodns-production --initial-cluster-state new
```

Publish each new Zone revision from the machine holding the authoritative zone
directory. The command checks zone files, stores the snapshot in etcd, then
atomically makes it current:

```sh
geodns-controller -mode ha -publish -config /srv/geodns/zones \
  -configfile /srv/geodns/controller/geodns.controller.conf \
  -etcd http://10.80.0.11:2379,http://10.80.0.12:2379,http://10.80.0.13:2379 \
  -etcd-prefix /geodns/production
```

Start each controller with the same etcd settings, a distinct `-id`, and its
own private `-http` address. For example, the first controller can use:

```sh
geodns-controller -mode ha -id controller-1 -config /srv/geodns/controller \
  -configfile geodns.controller.conf -http 10.80.0.11:8053 \
  -etcd http://10.80.0.11:2379,http://10.80.0.12:2379,http://10.80.0.13:2379 \
  -etcd-prefix /geodns/production
```

On each PoP, use the
[`dns/geodns.follower.ha.conf.sample`](dns/geodns.follower.ha.conf.sample)
pattern: set `urls` to all controller HTTP(S) origins, a unique node `id`, and
the shared token. The node tries the next controller on disconnect and preserves
its last applied zones while no leader is available. The leader persists known
node status every five seconds; a new leader initially marks those nodes offline
until they reconnect. An abrupt controller failure requires its ten-second etcd
lease to expire, followed by node reconnection. The HA mode requires an
etcd quorum; `-etcd-user`, `-etcd-password-file`, `-etcd-ca`, `-etcd-cert`, and
`-etcd-key` are available for authenticated TLS deployments. Restrict controller
and etcd ports to the private network. etcd should have persistent storage,
backups and compaction configured. This feature does not configure WireGuard,
probe DNS service availability, or control BGP announcements.
A single WireGuard hub remains a separate failure point even with three etcd
members; provide redundant tunnel paths if controller failover must survive a
hub outage. A controller candidate can run on the same host as a PoP's GeoDNS
process, but it uses a separate config directory and never serves DNS itself.

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
