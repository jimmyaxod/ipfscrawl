# ipfscrawl
IPFS crawler

Very simple IPFS crawler to see what's going on with IPFS.

This is a very general purpose libp2p-kad-dht crawler implementation. It crawls around the DHT by periodically sending out
FIND_NODE requests for random hashes. Peers reply with CloserPeers, which we then connect to and so the process repeats.

## Configuration

We start up a number of hosts, so that we get many different peerIDs.
We can also vary the maximum time we stay connected to each peer, and the frequency we send out FIND_NODE and PING messages.

## Run

`go run .`

## Output data

Whilst crawling, several output files are created. All are csv files with an initial field of unixtimestamp.

peerinfo - peerID_from, new_peerID, multiaddr

peerprotocols - peerID, protocol

peeragents - peerID, AgentVersion

peerids - peerID, id_name, id_length, id_digest

addproviders - peerID, CID, providerPeerID, multiaddr

getproviders - peerID, CID

## Performance

Here are some numbers from 5 minutes uptime on a home residential connection.

``
DHT uptime=300.73s active=718 cons=4828 streams=9902 total_peers_found=274648
Connections out=14877 (110539 fails) (122757 dupes) in=714
Writes ping=55394 find_node=55389
Reads put=0 get=0 addprov=3 getprov=13 find_node=14261 ping=14546
``

Unique peer IDs found: 2682

# Key types
``
   1193 identity
   1489 sha2-256
``

# Protocols (where known)
``
    309 /ipfs/ping/1.0.0
    309 /ipfs/kad/1.0.0
    309 /ipfs/id/push/1.0.0
    309 /ipfs/id/1.0.0
    306 /libp2p/circuit/relay/0.1.0
    299 /p2p/id/delta/1.0.0
    286 /libp2p/autonat/1.0.0
    278 /ipfs/bitswap/1.1.0
    278 /ipfs/bitswap/1.0.0
    278 /ipfs/bitswap
    277 /x/
    267 /ipfs/lan/kad/1.0.0
    267 /ipfs/bitswap/1.2.0
     57 /floodsub/1.0.0
     56 /meshsub/1.0.0
     55 /meshsub/1.1.0
     39 /libp2p/fetch/0.0.1
     26 /sbptp/1.0.0
     25 /sfst/1.0.0
      1 /thread/0.0.1
      1 /sbpcp/1.0.0
      1 /idena/gossip/1.0.0
``

# Top agent versions
``
     82 go-ipfs/0.7.0/
     32 go-ipfs/0.8.0/
     25 go-ipfs/0.7.0/ea77213
     22 go-ipfs/0.8.0/ce693d7
     21 go-ipfs/0.8.0/48f94e2
     20 go-ipfs/0.7.0/ea77213e3
     19 go-ipfs/0.6.0/
     17 go-ipfs/0.8.0-rc2/4080333
     15 storm
      7 go-ipfs/0.8.0/ce693d7e8
      4 go-ipfs/0.7.0/db62f0e34-dirty
      4 go-ipfs/0.7.0/0953ef945
      4 go-ipfs/0.4.23/
      3 go-ipfs/0.6.0/d6e036a
      3 go-ipfs/0.5.0/
      3 go-ipfs/0.4.22/
      2 hydra-booster/0.7.3
      2 go-ipfs/0.9.0-dev/6d76c6e
      2 go-ipfs/0.8.0-rc2/
      2 go-ipfs/0.8.0-rc1/02d15ac
``

