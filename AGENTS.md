# Repository Guidelines

## Project Structure & Module Organization

This is a Fabric 1.20.4 mod targeting Java 17. Common and server code lives under `src/main/java/imsng/player_to_player/`, organized by responsibility (`core`, `netproto`, `p2p`, `server`, `proxy`, `group`, `registry`, `env`, and related packages). Client-only code belongs in `src/client/java/.../client/`. Fabric metadata, mixin configs, and assets are split between `src/main/resources/` and `src/client/resources/`. See `docs/DESIGN.md` and `player_to_player-prompt.txt` for architecture details. Treat `build/`, `run/`, and `.gradle/` as generated directories.

## Build, Test, and Development Commands

Use the checked-in Gradle wrapper; on Unix, replace `.\gradlew.bat` with `./gradlew`.

- `.\gradlew.bat build`: compiles, remaps, and runs configured checks. The mod JAR is written to `build/libs/`.
- `.\gradlew.bat test`: runs Gradle tests; no automated test suite is currently committed.
- `.\gradlew.bat runClient`: launches a development Minecraft client.
- `.\gradlew.bat runServer`: launches a development dedicated server.
- `.\gradlew.bat genSources`: generates Minecraft sources for IDE navigation.

## Coding Style & Naming Conventions

No formatter or linter is enforced. Preserve the indentation of files you touch and avoid repository-wide formatting; existing files use both tabs and four spaces. For new Java files, use four spaces, same-line opening braces, and standard Java names: `PascalCase` types, `camelCase` methods and fields, and `UPPER_SNAKE_CASE` constants. Keep established identifiers such as `player_to_player` unchanged because metadata and package paths depend on them. Never reference client-only classes from `src/main`.

## Testing Guidelines

Place future unit tests in `src/test/java/`, mirroring production packages, and name them `*Test`. There is no numeric coverage requirement. Run `build` before every PR. For networking, chunk ownership, merge/split, or lifecycle changes, also smoke-test the relevant client/server roles and record the scenario and result.

## Commit & Pull Request Guidelines

Recent history uses terse release-version subjects such as `0.0.9.8` and `1.0.`. Reserve version-only subjects for releases; use a concise imperative subject for development commits. PRs should explain behavior changes, affected node roles, test commands and results, linked issues, and any config or protocol migration. Include screenshots for visible client changes and redacted logs for networking failures.

## Configuration & Protocol Safety

Do not commit generated worlds, runtime configs, IP addresses, relay endpoints, or database credentials. Treat changes to `Protocol.VERSION`, message types, mixin declarations, and `fabric.mod.json` as compatibility-sensitive and call them out explicitly in the PR.
