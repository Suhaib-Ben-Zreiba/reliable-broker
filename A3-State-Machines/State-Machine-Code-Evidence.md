# State Machine Code Evidence

Source: uploaded `sysc-termproject-starter-main` ZIP (official JPacman starter).  
Report placement: **SM-01 → Section 1.1, Figure 1.1**; **SM-02 → Section 1.2, Figure 1.3**.

Java source was not modified. Line numbers below were verified against that ZIP.

---

## SM-01 · Player State Machine

### Behaviour summary

`Player` stores lifecycle in the boolean field `alive`. The constructor sets `alive = true` (initial state **Alive**). `setAlive(false)` restarts the death animation and assigns `alive = false` (**Alive → Dead**). `setAlive(true)` stops the death animation, clears `killer`, and assigns `alive = true` (**Dead → Alive**); that branch is implemented but has **no production callers** in the starter. Gameplay death goes through `PlayerCollisions.playerVersusGhost` (and the equivalent path in `DefaultPlayerInteractionMap`), which calls `setAlive(false)` then `setKiller(ghost)`. There is **no guard**: the boolean parameter selects which action block runs. Repeated `setAlive(false)` while already Dead does not change state but still re-runs `deathSprite.restart()`.

### Evidence table

| State / Event / Transition | Java class | Method / Field | Line number |
|---|---|---|---|
| State field `alive` | `Player` | `private boolean alive` | 35 |
| Initial → Alive (`alive = true`) | `Player` | `Player(...)` constructor | 52 |
| Query Alive/Dead | `Player` | `isAlive()` | 63–65 |
| Event `setAlive(false)` / `deathSprite.restart()`; assign `alive` | `Player` | `setAlive(boolean)` | 75–84 (false branch 80–81; assign 83) |
| Event `setAlive(true)` / `deathSprite.setAnimating(false); killer = null` | `Player` | `setAlive(boolean)` | 75–84 (true branch 76–78; assign 83) |
| Gameplay caller: ghost collision | `PlayerCollisions` | `playerVersusGhost` → `setAlive(false)` | 76–78 |
| Related action after death | `PlayerCollisions` | `setKiller(ghost)` | 79 |
| Alternate collision map (same death call) | `DefaultPlayerInteractionMap` | `playerVersusGhost` → `setAlive(false)` | 52 |
| `setAlive(true)` production callers | — | none in starter `src/` | — |

### Limitations

- Debugger trace screenshots are a separate Task A deliverable and are **not** included here.
- `setAlive(true)` is shown because the method supports it; it is unused by gameplay in this codebase.

---

## SM-02 · Level State Machine

### Behaviour summary

`Level` stores lifecycle in `inProgress`. The constructor sets `inProgress = false` (initial state **Stopped**). `start()` is ignored if already running (early return — **not** a guard); otherwise it calls `startNPCs()`, sets `inProgress = true`, and `updateObservers()` (**Stopped → Running**). `stop()` is ignored if already stopped; otherwise it calls `stopNPCs()` and sets `inProgress = false` (**Running → Stopped**). While **Running**, `move(unit, direction)` is modelled as two **internal transitions** with complementary guards from `destination.isAccessibleTo(unit)`: accessible → `setDirection(); occupy(); collide(); updateObservers()`; not accessible → `setDirection(); updateObservers()`. Calls while Stopped return immediately (`!isInProgress()` — ignored, not a guard). `updateObservers()` may **send** `levelLost()` / `levelWon()` to observers — notifications, **not** stored Level states.

### Evidence table

| State / Event / Transition | Java class | Method / Field | Line number |
|---|---|---|---|
| State field `inProgress` | `Level` | `private boolean inProgress` | 54 |
| Initial → Stopped (`inProgress = false`) | `Level` | `Level(...)` constructor | 100 |
| Query Running/Stopped | `Level` | `isInProgress()` | 258–260 |
| Event `start()` / `startNPCs(); inProgress = true; updateObservers()` | `Level` | `start()` | 201–210 |
| Ignored `start()` when already Running | `Level` | `start()` early return | 203–205 |
| `startNPCs()` schedules ghosts | `Level` | `startNPCs()` | 229–238 |
| Event `stop()` / `stopNPCs(); inProgress = false` | `Level` | `stop()` | 216–224 |
| Ignored `stop()` when already Stopped | `Level` | `stop()` early return | 218–220 |
| `stopNPCs()` shuts down ghost executors | `Level` | `stopNPCs()` | 244–250 |
| Internal `move` `[destination.isAccessibleTo(unit)]` / setDirection; occupy; collide; updateObservers | `Level` | `move` + accessible branch | 172–195 (setDirection 182; occupy/collide 186–191; updateObservers 193) |
| Internal `move` `[!destination.isAccessibleTo(unit)]` / setDirection; updateObservers | `Level` | `move` when not accessible | 182; 186 false → 193 |
| `move` ignored when Stopped | `Level` | `if (!isInProgress()) return` | 177–179 |
| Send `levelLost()` / `levelWon()` | `Level` | `updateObservers()` | 265–276 |
| Observer contract | `Level.LevelObserver` | `levelWon()` / `levelLost()` | 361–373 |

### Limitations

- Won/Lost are intentionally **not** states of `Level`.
- Debugger verification of `Level.stop()` / `start()` is separate and unverified in this package.

---

## Visual / iteration notes

Issues found in earlier renders and corrected before final export:

1. Bidirectional transition labels colliding on one arc → switched to vertical layout with opposing left/right arcs.
2. Over-long curved arrows / excessive whitespace from `left to right` + `up`/`down` routing → removed directional routing hacks; used `top to bottom` with moderate `nodesep`/`ranksep`.
3. Evidence text crowding the figure → moved detailed evidence into this document; diagrams keep short `event / action` labels only.
4. Navy palette applied consistently (`#365879` arrows/borders, `#EAF2FA` fills, `#243B53` text, white background).

Final PNG files match the PlantUML sources in this directory (`SM-01-Player.puml` → `SM-01-Player.png`, `SM-02-Level.puml` → `SM-02-Level.png`).
