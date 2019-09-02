# DiscV5 Test Scenarios


## Scenario 1: Basic Test

### Setting

A: knows B's current ENR with endpoint, has B in routing table
B: knows A's current ENR with endpoint, has A in routing table, started first

### Expected result

- B repeatedly tries to ping A, which fails because it isn't started yet
- Once started, A pings B
- B responds with pong
- A and B continue to ping each other and respond with pongs
- No ENR updates are requested

### Actual result

works as expected


## Scenario 2: Add peer to routing table when pinged

### Setting

A: knows B's current ENR with endpoint, has B in routing table
B: knows A's current ENR with endpoint, empty routing table, started first

### Expected result

- B doesn't ping anyone as his routing table is empty
- B adds A to his routing table when pinged

### Actual result

works as expected


## Scenario 3: Recipient receives ENR during handshake if outdated

### Setting

A: knows B's current ENR with endpoint, has B in routing table
B: knows A's outdated ENR with endpoint, empty routing table, started first

### Expected result

- B updates A's ENR in his database

### Actual result

works as expected


## Scenario 4: Initiator requests ENR update if outdated

### Setting

A: knows B's outdated ENR with endpoint, has B in routing table
B: knows A's current ENR with endpoint, empty routing table, started first

### Expected result

- After B's first Ping, A requests B's ENR with find node message
- B replies with Nodes message
- A updates B's ENR in her database

### Actual result

works as expected


## Scenario 5: Recipient receives ENR during handshake if missing

### Setting

A: knows B's current ENR with endpoint, has B in routing table
B: doesn't know A's ENR, empty routing table, started first

### Expected result

- B updates A's ENR in his database

### Actual result

works as expected


## Scenario 6: Update own endpoint information

### Setting

A: knows B's current ENR with endpoint, has B in routing table, own endpoint is missing
B: doesn't know A's ENR, empty routing table, started first

### Expected result
- B receives A's ENR without endpoint during handshake
- On his first ping, B includes A's endpoint information in pong
- A updates their ENR accordingly
- A notifies B of ENR update in next ping/pong
- B requests A's new ENR and stores it in db

### Actual result

works as expected
