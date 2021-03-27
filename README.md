# ipfscrawl
IPFS crawler

Very simple IPFS crawler to see what's going on with IPFS and as a learning exersize.

This is a very general purpose libp2p-kad-dht crawler implementation. It crawls around the DHT by periodically sending out
FIND_NODE requests for random hashes. Peers reply with CloserPeers, which we then connect to and so the process repeats.
This crawler operates at a low level in order to squeeze out more efficiency and flexibility.

## Configuration

We start up a number of hosts, so that we get many different peerIDs.
We can also vary the maximum time we stay connected to each peer, and the frequency we send out FIND_NODE and PING messages.

## Run

`go run .`

## Output data

Whilst crawling, several output files are created. All are csv files with an initial field of unixtimestamp. Periodically (default 1 hour) the output files are changed.

| Filename      | Fields                                    |
| ------------- | ----------------------------------------- |
| peerinfo      | peerID_from, new_peerID, multiaddr        |
| peerprotocols | peerID, protocol                          |
| peeragents    | peerID, AgentVersion                      |
| peerids       | peerID, id_name, id_length, id_digest     |
| addproviders  | peerID, CID, providerPeerID, multiaddr    |
| getproviders  | peerID, CID                               |
| get           | peerID, key                               |
| get_ipns      | peerID, ID                                |
| put           | peerID, key, rec_key, val, time_recvd     |
| put_ipns      | peerID, pid, val, ttl, seq                |

## Data

This is a limited 3 hour crawl, showing provider queries

| Item                    | value     |
| ----------------------- | --------- |
| Raw addProvider calls   |   223,166 |
| Unique addProvider      |   208,333 |


# Key types

| multihash type | peers |
| -------------- | ----- |
| identity       | 1193  |
| sha2-256       | 1489  |

# Supported protocols (where known)

| Protocol                    | peers |
| --------------------------- | ----- |
| /ipfs/ping/1.0.0            |   309 |
| /ipfs/kad/1.0.0             |   309 |
| /ipfs/id/push/1.0.0         |   309 |
| /ipfs/id/1.0.0              |   309 |
| /libp2p/circuit/relay/0.1.0 |   306 |
| /p2p/id/delta/1.0.0         |   299 |
| /libp2p/autonat/1.0.0       |   286 |
| /ipfs/bitswap/1.1.0         |   278 |
| /ipfs/bitswap/1.0.0         |   278 |
| /ipfs/bitswap               |   278 |
| /x/                         |   277 |
| /ipfs/lan/kad/1.0.0         |   267 |
| /ipfs/bitswap/1.2.0         |   267 |
| /floodsub/1.0.0             |    57 |
| /meshsub/1.0.0              |    56 |
| /meshsub/1.1.0              |    55 |
| /libp2p/fetch/0.0.1         |    39 |
| /sbptp/1.0.0                |    26 |
| /sfst/1.0.0                 |    25 |
| /thread/0.0.1               |     1 |
| /sbpcp/1.0.0                |     1 |
| /idena/gossip/1.0.0         |     1 |

# Top agent versions

| Agent version                 | peers |
| ----------------------------- | ----- |
| go-ipfs/0.7.0/                |    82 |
| go-ipfs/0.8.0/                |    32 |
| go-ipfs/0.7.0/ea77213         |    25 |
| go-ipfs/0.8.0/ce693d7         |    22 |
| go-ipfs/0.8.0/48f94e2         |    21 |
| go-ipfs/0.7.0/ea77213e3       |    20 |
| go-ipfs/0.6.0/                |    19 |
| go-ipfs/0.8.0-rc2/4080333     |    17 |
| storm                         |    15 |
| go-ipfs/0.8.0/ce693d7e8       |     7 |
| go-ipfs/0.7.0/db62f0e34-dirty |     4 |
| go-ipfs/0.7.0/0953ef945       |     4 |
| go-ipfs/0.4.23/               |     4 |
| go-ipfs/0.6.0/d6e036a         |     3 |
| go-ipfs/0.5.0/                |     3 |
| go-ipfs/0.4.22/               |     3 |
| hydra-booster/0.7.3           |     2 |
| go-ipfs/0.9.0-dev/6d76c6e     |     2 |
| go-ipfs/0.8.0-rc2/            |     2 |
| go-ipfs/0.8.0-rc1/02d15ac     |     2 |

