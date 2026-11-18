# `Hasql.Pool` — structure, STM transactions and slot accounting

Format: Mermaid, so it renders on GitHub, VS Code and `mmdc`.
- **Flowcharts** for control flow inside each function.
- **State diagram** for the lifecycle of a capacity slot. This is where the invariant lives.
- **Sequence diagram** for the `use`/`release` interaction.

Legend used in every flowchart:

| Shape / colour | Meaning |
|---|---|
| blue box (`stm`) | code running **inside** an `atomically` block (`STM#n`) |
| orange box (`io`) | plain `IO` that is **outside** STM (it can be interrupted, can throw, and is not atomic) |
| red box (`risk`) | point where the invariant can break (leak or inconsistent state) |
| `⟲ retry` | STM `retry` (blocks until a TVar that was read changes) |

---

## 0. The `Pool` record and who touches what

```mermaid
---
title: "Pool record: fields and which functions read or write them"
---
flowchart LR
  classDef legendTxt fill:#fff,stroke:#fff,color:#000
  classDef immut fill:#eee,stroke:#999,color:#000
  classDef tvar fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef ioref fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef fn fill:#fff,stroke:#000,color:#000,font-weight:bold

  subgraph Pool
    size["poolSize :: Int"]:::immut
    settings["poolConnectionSettings"]:::immut
    acqT["poolAcquisitionTimeout µs"]:::immut
    maxLife["poolMaxLifetime ns"]:::immut
    maxIdle["poolMaxIdletime ns"]:::immut
    Q["poolConnectionQueue :: TQueue Entry"]:::tvar
    CAP["poolCapacity :: TVar Int"]:::tvar
    RV["poolReuseVar :: TVar (TVar Bool)<br/>(generation flag)"]:::tvar
    REF["poolReaperRef :: IORef ()<br/>(weak ref → kills manager)"]:::ioref
    OBS["poolObserver"]:::immut
  end

  acquire([acquire]):::fn
  manager([manager thread<br/>forked by acquire]):::fn
  use([use]):::fn
  release([release]):::fn

  acquire -- creates all --> Pool
  acquire -. "Config.size (NOT poolSize field)" .-> CAP

  manager -- "flush / write" --> Q
  manager -- "succ (per reaped)" --> CAP
  manager -. "closes over config, not Pool" .-> maxLife

  use -- "readTQueue / unGetTQueue" --> Q
  use -- "pred (new conn) / succ (drop)" --> CAP
  use -- "read (snapshot gen)" --> RV
  use -- "read captured gen flag" --> GEN["captured inner TVar Bool"]:::tvar
  use --> acqT & maxLife & maxIdle & settings & OBS

  release -- "flush" --> Q
  release -- "succ (per entry)" --> CAP
  release -- "read + write new gen" --> RV
  release -- "write False" --> GEN
  release --> OBS

  REF -. "GC finalizer: killThread" .-> manager

  subgraph LEGEND["Legend"]
    direction TB
    lg1["grey: immutable field"]:::immut
    lg2["blue: TVar / TQueue (STM state)"]:::tvar
    lg3["orange: IORef (non-STM)"]:::ioref
    lg4(["rounded bold: function / thread"]):::fn
    lg5["solid arrow: reads or writes<br/>dashed arrow: indirect relation"]:::legendTxt
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  GEN ~~~ LEGEND
```

Notes:
- `poolSize` is **never read** after construction. The invariant
  `poolSize == capacity + length queue + inFlight + inLimbo` exists only in a comment and nothing checks it.
- The manager thread does **not** read the `Pool` record. It closes over `config` and the raw `TVar`s. This
  matters for the weak-ref trick: if the manager held the `Pool`, the pool would never be garbage collected.

### Where a connection slot can be (invariant view)

```mermaid
---
title: "Lifecycle of one capacity slot: where it lives and which STM block moves it"
---
stateDiagram-v2
  direction LR
  [*] --> Capacity : acquire (cap = size)
  Capacity --> Connecting : use STM#U1/U2 tryNewConnection (cap--)
  Connecting --> InFlight : Connection.acquire Right
  Connecting --> Capacity : Connection.acquire Left → STM#U3 (cap++)
  Queued --> Checking : use STM#U1/U2 readTQueue
  Checking --> InFlight : fresh (entryUseTime = now)
  Checking --> Connecting : aged/idle → Connection.release (slot reused, cap untouched)
  InFlight --> Queued : returnConn STM#U4 reuse=True (unGetTQueue, front)
  InFlight --> Closing : returnConn reuse=False, or reset-class session error
  Closing --> Capacity : STM#U5/U6 (cap++)
  Queued --> Reaping : manager STM#M1 flush (aged/idle)
  Queued --> Queued : manager STM#M1 flush + rewrite live (goes to back)
  Reaping --> Capacity : manager STM#M2 (cap++)
  Queued --> Releasing : release STM#R1 flush
  Releasing --> Capacity : release STM#R2 (cap++)

  note right of Connecting : slot held in a local variable only.\nAn async exception here loses the slot forever.
  note right of Checking : same risk, Connection.release may throw.
  note right of InFlight : slot held by the user thread.
  note right of Reaping : slot is not in the queue and not in cap.\nNothing else can see it.
  note right of Releasing : same as Reaping

  classDef shared fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef local fill:#ffc9c9,stroke:#c92a2a,color:#000
  class Capacity, Queued shared
  class Connecting, Checking, InFlight, Closing, Reaping, Releasing local

  state "Legend" as Legend {
    direction TB
    L_shared : blue = slot visible in shared STM state (CAP or Q)
    L_local : red = slot held only in a thread-local variable
    L_arrow : arrow label = code path and STM block id (M, R, U)
  }
  class L_shared shared
  class L_local local
```

Every state except `Capacity` and `Queued` exists only in a Haskell local variable. Nothing sees those slots
again unless the code reaches the matching `cap++`. The code does not use `mask`, `bracket`,
`onException` or `finally`.

---

## 1. `acquire`, including the manager (reaper) thread

STM blocks: **M1** (once per second), **M2** (once per reaped entry).

```mermaid
---
title: "acquire: pool construction and manager (reaper) thread, STM blocks M1 and M2"
---
flowchart TD
  classDef stm fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef io fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef risk fill:#ffc9c9,stroke:#c92a2a,color:#000
  classDef legendTxt fill:#fff,stroke:#fff,color:#000

  A0["newTQueueIO, newTVarIO size,<br/>newTVarIO (newTVarIO True), newIORef ()"]:::io
  A0 --> FORK["forkIOWithUnmask (unmask $ forever ...)"]:::io
  FORK --> WEAK["mkWeakIORef reaperRef (killThread managerTid)"]:::io
  WEAK --> RET["return Pool{...}"]:::io

  FORK -.-> L0
  subgraph manager["manager thread loop"]
    L0["threadDelay 1s; now ← monotonic"]:::io --> M1
    subgraph M1["STM#M1  touches: Q"]
      F1["entries ← flushTQueue Q"]:::stm --> P1["partition aged / idle / live"]:::stm
      P1 --> W1["writeTQueue Q live (appended to back,<br/>so the order differs from unGet LIFO)"]:::stm
    end
    M1 --> REAP{{"for each aged, then each idle entry"}}
    REAP --> R1["catchSync Connection.release"]:::io
    R1 --> M2["STM#M2  touches: CAP<br/>modifyTVar' CAP succ"]:::stm
    M2 --> R2["observe Terminated"]:::io
    R2 --> REAP
    REAP -->|done| L0
  end

  R1 -. "async exc (killThread on GC)<br/>→ remaining flushed entries are never closed<br/>(harmless only because the pool is dead)" .-> X1["leak"]:::risk
  M1 -. "between M1 and M2, reaped entries are<br/>in neither Q nor CAP" .-> X2["transient invariant gap"]:::risk

  subgraph LEGEND["Legend"]
    direction TB
    lg1["blue: inside an STM atomically block"]:::stm
    lg2["orange: plain IO outside STM<br/>(interruptible, may throw)"]:::io
    lg3["red: invariant may break<br/>(leak or dirty state)"]:::risk
    lg4(["rounded: entry / exit / call target"])
    lg5{"diamond: decision"}
    lg6{{"hexagon: loop"}}
    lg7["framed box: STM transaction or scope<br/>solid arrow: control flow<br/>dashed arrow: failure / risk annotation<br/>⟲ : STM retry (blocks or falls through)"]:::legendTxt
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5 ~~~ lg6 ~~~ lg7
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  R2 ~~~ LEGEND
```

---

## 2. `release`

STM blocks: **R1** (once), **R2** (once per idle entry).

```mermaid
---
title: "release: generation switch (R1) and closing idle connections (R2)"
---
flowchart TD
  classDef stm fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef io fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef risk fill:#ffc9c9,stroke:#c92a2a,color:#000
  classDef legendTxt fill:#fff,stroke:#fff,color:#000

  subgraph R1["STM#R1  touches: RV, oldGen, newGen, Q"]
    a["prev ← readTVar RV"]:::stm --> b["writeTVar prev False<br/>(in-flight conns of old gen get closed on return)"]:::stm
    b --> c["new ← newTVar True; writeTVar RV new"]:::stm
    c --> d["entries ← flushTQueue Q"]:::stm
  end
  R1 --> LOOP{{"forM_ entries"}}
  LOOP --> e["Connection.release"]:::io
  e --> R2["STM#R2  touches: CAP<br/>modifyTVar' CAP succ"]:::stm
  R2 --> f["observe Terminated Release"]:::io --> LOOP

  e -. "sync or async exception<br/>→ the rest of the entries are neither closed<br/>nor credited back to CAP (permanent capacity loss)" .-> X["leak"]:::risk

  subgraph LEGEND["Legend"]
    direction TB
    lg1["blue: inside an STM atomically block"]:::stm
    lg2["orange: plain IO outside STM<br/>(interruptible, may throw)"]:::io
    lg3["red: invariant may break<br/>(leak or dirty state)"]:::risk
    lg4(["rounded: entry / exit / call target"])
    lg5{"diamond: decision"}
    lg6{{"hexagon: loop"}}
    lg7["framed box: STM transaction or scope<br/>solid arrow: control flow<br/>dashed arrow: failure / risk annotation<br/>⟲ : STM retry (blocks or falls through)"]:::legendTxt
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5 ~~~ lg6 ~~~ lg7
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  f ~~~ LEGEND
```

R1 is a single correct atomic step. All the bookkeeping that follows it is unprotected.

---

## 3. `use` (main focus)

STM blocks:

| id | where | TVars read | TVars written | can `retry`? |
|---|---|---|---|---|
| **U1** | fast path, `optional (...)` | RV, Q, CAP | Q (pop) **or** CAP (`-1`) | no (turned into `Nothing`) |
| **U2** | slow path, `join . atomically` | RV, Q, CAP, delay TVar | Q (pop) **or** CAP (`-1`) | yes, wakes on any change to Q/CAP/delay |
| **U3** | `acquireNewConnection`, connect failed | CAP | CAP `+1` | no |
| **U4** | `returnConn` | captured gen flag, Q | Q (`unGetTQueue`, front) | no |
| **U5** | `returnConn`, generation is stale | CAP | CAP `+1` | no |
| **U6** | session error classified as `reset` | CAP | CAP `+1` | no |

### 3a. Acquisition phase

```mermaid
---
title: "use (1/3): acquiring a slot, fast path U1 and blocking path U2"
---
flowchart TD
  classDef stm fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef io fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef risk fill:#ffc9c9,stroke:#c92a2a,color:#000
  classDef legendTxt fill:#fff,stroke:#fff,color:#000

  START(["use pool sess"]) --> U1

  subgraph U1["STM#U1 (fast path)  touches: RV, Q, CAP"]
    u1a["gen ← readTVar RV"]:::stm --> u1b{"readTQueue Q"}:::stm
    u1b -- "got entry" --> u1ok1["Just (useAvailableConnection gen entry)"]:::stm
    u1b -- "empty ⟲" --> u1c{"CAP > 0 ?"}:::stm
    u1c -- yes --> u1d["CAP ← CAP-1<br/>Just (acquireNewConnection gen)"]:::stm
    u1c -- "no ⟲" --> u1n["optional → Nothing"]:::stm
  end

  u1ok1 --> ACT
  u1d --> ACT
  u1n --> D["delay ← registerDelay acqTimeout<br/>t0 ← now"]:::io --> U2

  subgraph U2["STM#U2 (slow path, blocks)  touches: RV, Q, CAP, delay"]
    u2a["gen ← readTVar RV (re-read)"]:::stm --> u2b{"readTQueue Q"}:::stm
    u2b -- entry --> u2ok1["observe wait; useAvailableConnection"]:::stm
    u2b -- "⟲" --> u2c{"CAP > 0 ?"}:::stm
    u2c -- yes --> u2ok2["CAP-1; observe wait; acquireNewConnection"]:::stm
    u2c -- "⟲" --> u2t{"readTVar delay"}:::stm
    u2t -- True --> u2to["observe wait; Left AcquisitionTimeout"]:::stm
    u2t -- "False ⟲ (block)" --> u2a
  end
  u2ok1 --> ACT
  u2ok2 --> ACT
  u2to --> END(["return"])

  ACT["join: run the returned IO action<br/>(slot now held ONLY by this thread's stack)"]:::risk
  ACT --> NEXT[[see 3b / 3c]]

  subgraph LEGEND["Legend"]
    direction TB
    lg1["blue: inside an STM atomically block"]:::stm
    lg2["orange: plain IO outside STM<br/>(interruptible, may throw)"]:::io
    lg3["red: invariant may break<br/>(leak or dirty state)"]:::risk
    lg4(["rounded: entry / exit / call target"])
    lg5{"diamond: decision"}
    lg6{{"hexagon: loop"}}
    lg7["framed box: STM transaction or scope<br/>solid arrow: control flow<br/>dashed arrow: failure / risk annotation<br/>⟲ : STM retry (blocks or falls through)"]:::legendTxt
    lg8[["double-edged: continues in another diagram"]]
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5 ~~~ lg6 ~~~ lg7 ~~~ lg8
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  END ~~~ LEGEND
```

### 3b. Obtaining a live connection

```mermaid
---
title: "use (2/3): getting a live connection, reuse or connect (U3)"
---
flowchart TD
  classDef stm fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef io fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef risk fill:#ffc9c9,stroke:#c92a2a,color:#000
  classDef legendTxt fill:#fff,stroke:#fff,color:#000

  UA(["useAvailableConnection gen entry"]) --> now["now ← monotonic"]:::io
  now --> aged{"aged?"}
  aged -- yes --> rel1["Connection.release; observe Aging"]:::risk --> ANC
  aged -- no --> idle{"idle?"}
  idle -- yes --> rel2["Connection.release; observe Idleness"]:::risk --> ANC
  idle -- no --> RS(["runSessionOnConnection gen entry{useTime=now}"])

  ANC(["acquireNewConnection gen"]) --> obs1["now, uuid, observe Connecting"]:::io
  obs1 --> conn["Connection.acquire settings<br/>(network I/O, may block for a long time)"]:::risk
  conn -- Left err --> U3["STM#U3  CAP+1"]:::stm --> L1(["Left ConnectionUsageError"])
  conn -- Right c --> obs2["observe Ready Established"]:::io --> RS2(["runSessionOnConnection gen (Entry c now now uuid)"])

  rel1 -. "throws → slot lost (no CAP+1)" .-> LEAK["capacity leak"]:::risk
  conn -. "async exc (System.Timeout, killThread,<br/>Warp request cancelled) or a sync exc<br/>→ slot lost, CAP never restored" .-> LEAK

  subgraph LEGEND["Legend"]
    direction TB
    lg1["blue: inside an STM atomically block"]:::stm
    lg2["orange: plain IO outside STM<br/>(interruptible, may throw)"]:::io
    lg3["red: invariant may break<br/>(leak or dirty state)"]:::risk
    lg4(["rounded: entry / exit / call target"])
    lg5{"diamond: decision"}
    lg6{{"hexagon: loop"}}
    lg7["framed box: STM transaction or scope<br/>solid arrow: control flow<br/>dashed arrow: failure / risk annotation<br/>⟲ : STM retry (blocks or falls through)"]:::legendTxt
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5 ~~~ lg6 ~~~ lg7
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  L1 ~~~ LEGEND
```

### 3c. Running the session and giving the slot back

```mermaid
---
title: "use (3/3): running the session and returning the slot (U4, U5, U6)"
---
flowchart TD
  classDef stm fill:#cfe3ff,stroke:#2a62c9,color:#000
  classDef io fill:#ffe9c7,stroke:#c98a2a,color:#000
  classDef risk fill:#ffc9c9,stroke:#c92a2a,color:#000
  classDef callRef fill:#f3e8ff,stroke:#7a3fc9,color:#000
  classDef legendTxt fill:#fff,stroke:#fff,color:#000

  RS(["runSessionOnConnection gen entry"]) --> o["observe InUse"]:::io
  o --> run["try @SomeException (Session.run sess conn)"]:::io
  run -- "Left exc (ANY exception, async included)" --> RC1["returnConn"]:::callRef --> rethrow(["throwIO exc"])
  run -- "Right (Left err)" --> cls{"ErrorsDestruction.reset<br/>classifies err"}
  run -- "Right (Right r)" --> RC3["returnConn"]:::callRef --> ok(["observe Ready; Right r"])

  cls -- "connection-level error" --> relE["Connection.release"]:::io --> U6["STM#U6  CAP+1"]:::stm --> e1(["observe Terminated; Left SessionUsageError"])
  cls -- "statement-level error" --> RC2["returnConn"]:::callRef --> e2(["observe Ready SessionFailed; Left SessionUsageError"])

  subgraph returnConn
    subgraph U4["STM#U4  touches: gen flag, Q"]
      g{"readTVar gen"}:::stm
      g -- True --> push["unGetTQueue Q entry (front, LIFO)"]:::stm
      g -- False --> defer["return closer action"]:::stm
    end
    defer --> relR["Connection.release"]:::io --> U5["STM#U5  CAP+1"]:::stm --> obsR["observe Terminated Release"]:::io
  end

  RC1 -. "conn returned to Q after an async exception<br/>interrupted libpq mid-query/transaction<br/>→ the next user may get a dirty/busy connection" .-> DIRTY["inconsistent conn state"]:::risk
  relE -. "throws → slot lost" .-> LEAK["capacity leak"]:::risk
  relR -. "throws, or async exc → slot lost" .-> LEAK
  run -. "2nd async exc while inside returnConn<br/>(not masked) → slot lost" .-> LEAK

  subgraph LEGEND["Legend"]
    direction TB
    lg1["blue: inside an STM atomically block"]:::stm
    lg2["orange: plain IO outside STM<br/>(interruptible, may throw)"]:::io
    lg3["red: invariant may break<br/>(leak or dirty state)"]:::risk
    lg4(["rounded: entry / exit / call target"])
    lg5{"diamond: decision"}
    lg6{{"hexagon: loop"}}
    lg7["framed box: STM transaction or scope<br/>solid arrow: control flow<br/>dashed arrow: failure / risk annotation<br/>⟲ : STM retry (blocks or falls through)"]:::legendTxt
    lg8["purple: call to returnConn (expanded in its frame)"]:::callRef
    lg1 ~~~ lg2 ~~~ lg3 ~~~ lg4 ~~~ lg5 ~~~ lg6 ~~~ lg7 ~~~ lg8
  end
  style LEGEND fill:#fafafa,stroke:#333,stroke-dasharray:4 2
  obsR ~~~ LEGEND
```

---

## 4. Concurrency: `use` racing `release` and the manager

```mermaid
---
title: "Concurrency: use racing release and the manager over the shared TVars"
---
sequenceDiagram
  autonumber
  participant U as use (thread A)
  participant Q as Q / CAP / RV
  participant R as release (thread B)
  participant M as manager

  U->>Q: STM U1 gen₁ ← RV, pop entry e
  Note over U: e in flight, tagged with gen₁
  R->>Q: STM R1 gen₁:=False, RV:=gen₂, flush Q
  R->>R: close flushed, STM R2 CAP+1 each
  M->>Q: STM M1 flush, rewrite live (sees no released entries)
  U->>U: Session.run on e
  U->>Q: STM U4 read gen₁ = False
  U->>U: Connection.release e
  U->>Q: STM U5 CAP+1
  Note over Q: correct. The generation TVar is the only<br/>cross-thread coordination and it is atomic.
  Note right of M: Legend<br/>vertical line: thread or shared state<br/>arrow to Q: STM transaction (block id)<br/>self-arrow: plain IO on own thread<br/>numbers: order of events
```

The generation trick is sound because each of U1, U2 and U4 reads `RV` (or the captured flag) in the same
transaction as the queue operation. The races that remain are between the STM blocks, not inside them.

---

## 5. Findings

### Safety (in order of severity)

1. **No async-exception masking anywhere in `use`.** After U1 or U2 commits, the slot (a `CAP` decrement or a
   popped `Entry`) is held only on the thread's stack. If an async exception lands during
   `Connection.acquire`, `Connection.release`, the observers or `returnConn` (`timeout`, `killThread`,
   Warp cancellation), the slot is lost for good. Repeated losses shrink the pool towards 0, after which
   every `use` returns `AcquisitionTimeout`. Fix: `mask` around acquire → act → return, using
   `bracketOnError`/`onException` so every exit path does `CAP+1`.
2. **`try @SomeException` catches async exceptions, then returns the connection to the pool**
   (`Pool.hs:225-230`). A `Session.run` that is cancelled half-way leaves libpq busy or inside an open
   transaction, and the next borrower gets that connection. Safer behaviour: on an async exception, close the
   connection (U6-style) and do not `unGetTQueue` it.
3. **`release` loop is not exception-safe** (`Pool.hs:136-139`). A throwing `Connection.release` aborts the
   loop, so the remaining connections are neither closed nor credited back to `CAP`.
4. **Same pattern in the "close then credit" paths** (aged/idle in `useAvailableConnection`, U5, U6):
   `Connection.release` runs before `CAP+1`, so an exception between the two leaks the slot.
5. **Reaper/release invariant gaps** (M1→M2, R1→R2): slots are temporarily in neither `Q` nor `CAP`. This is
   harmless for correctness but makes any capacity metric inaccurate.
6. **Thundering herd**: each waiter in U2 retries on any write to `Q` or `CAP`, so N waiters re-run N
   transactions per event. This is a performance problem, not a correctness one.

### Design complexity

- **Mutable state:** 3 shared (`Q`, `CAP`, `RV`), plus one generation TVar per `release`, plus one delay TVar
  per waiting `use`.
- **Distinct `atomically` sites:** 9 (M1, M2, R1, R2, U1–U6). `use` alone has **6**, and one call to `use`
  runs between **2** (U1 + U4) and **3** of them (e.g. U2 + U3, or U1 + U4 + U5).
- **Exit paths from `use` that must restore the invariant:** 7 (timeout, connect-fail, ok, statement-error,
  reset-error, exception, stale-generation). Each one restores it by hand. Because no single `bracket`
  owns the slot, every new branch is one more place where a leak is possible.
- **The invariant `size = CAP + |Q| + inFlight + inLimbo`** is enforced only by convention. Nothing checks
  it, and `poolSize` is never read again.

A smaller design would be one `bracket`-style `withSlot` (acquire: U1/U2; release: one STM step that either
`unGetTQueue`s or does `CAP+1`, chosen by the generation flag), run under `mask` with `restore` around
`Session.run`. That replaces U3–U6 and the hand-written exit paths with a single release point.
