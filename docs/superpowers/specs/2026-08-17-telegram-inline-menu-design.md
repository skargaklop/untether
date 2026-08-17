# Telegram Inline Menu Design

## Contract
`/menu` opens a fixed inline keyboard with eight full-width rows:

1. New (`menu:new`)
2. Settings (`menu:config`)
3. Model (`menu:model`)
4. Topic (`menu:topic`)
5. Stats (`menu:stats`)
6. Engines (`menu:engines`)
7. Compact (`menu:compact`)
8. Cancel (`untether:cancel`)

Context and Threads are intentionally absent.

## Routing
Every `menu:<action>` is resolved through a closed mapping and replayed as the corresponding existing slash command. Callback payloads cannot supply arbitrary command text. Cancel retains the existing cancellation callback path.

## Engines
Engines replays `/config ag`. The existing engine chooser lists `runtime.available_engine_ids()` only, preserving existing selection persistence and including launchable dynamic ACP engines such as Cline.
