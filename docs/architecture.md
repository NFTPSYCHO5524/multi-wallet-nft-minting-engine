# Architecture and execution flow

The minting system is built around preparing work before the trigger, then keeping the submission path focused on sending already prepared transactions.

## Components

| Module | Work handled |
| --- | --- |
| `mintbot/runner.py` | Builds, signs and submits mint attempts |
| `mintbot/rpc.py` | Provider requests, health probes, routing, HTTP connection management and receipt polling |
| `mintbot/trigger.py` | Time-based and contract-condition launch triggers |
| `mintbot/walletstore.py` | Encrypted wallet storage and selection |
| `telegram_bot.py` | Command handling, launch controls and notifications |
| `mintbot/schedulestate.py` | Persistent schedule state |
| `mintbot/mintstate.py` | Tracking mint activity |

## Before the trigger

The bot can check configured RPC providers, probe candidate concurrent request widths, prepare contract calldata, sign an initial transaction attempt and warm connections.

These operations reduce how much preparation is left on the critical submission path.

## At the trigger

A scheduler or contract condition starts bounded asynchronous submission across the selected wallets. The RPC layer routes requests to providers while managing capacity. Receipt queries are handled separately so that slower confirmations do not occupy the same connector slots used for outgoing submissions.

## After submission

Receipt polling and state updates continue independently of the submission burst. The Telegram interface can report status and allow the operator to inspect the run.

## Engineering choices

**Bounded concurrency:** A larger number of simultaneous requests does not necessarily produce faster sends if providers become congested. The application tests submission lane widths rather than assuming the largest possible setting wins.

**Adaptive endpoint selection:** Endpoint responsiveness can change before a launch. Prewarm and health checks refresh those observations closer to the trigger.

**Isolated receipt transport:** Receipt polling has its own HTTP session and connector, reducing competition between follow-up requests and new submissions.

**Persistent state:** Scheduling and mint status are tracked outside the immediate request lifecycle so the process can report the run's progress.
