# Telegram Inline Menu Design

## Contract
`/menu` opens a fixed inline keyboard. Actions pair two buttons per row;
Cancel spans the final row on its own:

1. New (`menu:new`) | Settings (`menu:config`)
2. Model (`menu:model`) | Agent (`menu:agent`)
3. Topic (`menu:topic`) | Stats (`menu:stats`)
4. Engines (`menu:engines`) | Compact (`menu:compact`)
5. Queue (`menu:queue`) | Health (`menu:health`)
6. Cancel (`untether:cancel`)

Context and Threads are intentionally absent.

## Routing
Every `menu:<action>` is resolved through a closed mapping and replayed as the corresponding existing slash command. Callback payloads cannot supply arbitrary command text. Cancel retains the existing cancellation callback path.

## Engines
Engines replays `/config ag`. The existing engine chooser lists `runtime.available_engine_ids()` only, preserving existing selection persistence and including launchable dynamic ACP engines such as Cline.
