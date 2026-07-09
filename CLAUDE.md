# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build the mod
./gradlew build

# Run the Minecraft client (dev environment)
./gradlew runClient

# Run the Minecraft server (dev environment)
./gradlew runServer

# Generate IDE run configs
./gradlew genSources
```

Output JAR is at `build/libs/player_to_player-<version>.jar`.

## Stack

- Minecraft 1.20.4 + Fabric Loader 0.19.3 + Fabric API 0.97.3
- Java 17, Fabric Loom 1.17
- Split environment source sets: `src/main` (server-side) and `src/client` (client-only)

## Architecture

This is a **P2P distributed computing Minecraft mod**. The goal is to offload all server-side world tick computation to player clients, using P2P hole-punching to connect clients directly.

### Four node types

| Role | 角色 | Responsibility |
|------|------|---------------|
| `server` | 服务端 | Chunk registry, environment file distribution, MCA file writes, player table |
| `proxy_server` | 中转服务端 | P2P hole-punching assist, fallback relay, env file distribution |
| `primary_client` | 主客户端 | Runs world tick (main thread) for its loaded chunks; acts as mini-server for its group |
| `secondary_client` | 副客户端 | Renders and sends input only; no tick computation |

The mode is set in a global config file. Auto-detected on first load: server environment → `server`, client environment → `client`.

### Group client model

- A **group client** (组客户端) = one primary + its secondaries
- Each chunk can only be loaded by one group at a time
- The server maintains a **chunk registry** (chunk list / MySQL) mapping chunks → owning primary client

### Key workflows

- **Chunk load request**: client asks server if chunk + 4 adjacent chunks are free → if yes, load; if no, trigger **pre-connection** with the owning group
- **Pre-connection → pre-sync → merge** (预连接→预同步→合并): two groups merge into one; the higher-compute-score client becomes the new primary; target freeze time < 100ms
- **Split** (分离): when a secondary's render area has no overlap (including 4-chunk buffer) with the rest of the group for 10s, it becomes its own primary (runs pre-sync in background during that 10s)
- **Compute allocation** (算力分配): ranked by single-core CPU score (via Geekbench unofficial API) + available RAM ≥ 0.5 GB; top score = primary client
- **Environment sync**: SHA-256 hash check on join; server pushes env file diff if mismatch

### Mod file prefix convention (server-side distribution)

Prefixes on JAR filenames control which nodes load them:
- `server-` — server only
- `proxy_server-` — relay server only
- `server_client-` — primary client only
- `client-` — secondary client only
- No prefix → all nodes

A single JAR may have multiple prefixes (e.g. `server-server_client-client-mymod-1.0.jar`).

### World folder layout

```
player-to-player/
  config.json          # global config (mode, relay server IP, etc.)
  <IP>+<worldname>/    # per-server-per-world folder
    environment/       # env files for this world
    data/              # world data files
    config.json        # world-specific config
```

### Source layout

```
src/main/java/imsng/player_to_player/
  Player_to_player.java          # ModInitializer (server + common init)
src/client/java/imsng/player_to_player/client/
  Player_to_playerClient.java    # ClientModInitializer (client-only init)
src/main/resources/
  fabric.mod.json                # mod metadata & entrypoints
  player_to_player.mixins.json   # server-side mixin config
src/client/resources/
  player_to_player.client.mixins.json  # client-side mixin config
```

The detailed design spec is in `player_to_player-prompt.txt` (Chinese).
