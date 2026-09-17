---
name: keiba-v2-raceday
description: Operate the keiba_platform_v2 SHADOW raceday workflow safely on a real JRA meeting day. Use this skill when the user asks to run, continue, check, troubleshoot, or verify today's Keiba V2 raceday operation.
---

# Keiba V2 Raceday Operator

The user's explicit instructions take precedence over this skill.

## Scope

Operate only the standalone `keiba_platform_v2` project. Do not modify or run the legacy betting system unless the user explicitly asks.

This workflow is SHADOW-only. Do not place real bets or add a live betting gateway.

## Working directory

Run commands from the `keiba_platform_v2` project root.

Prefer the wrapper:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\raceday_operator.ps1 -Action <Action> -RaceDate YYYYMMDD
```

If `-RaceDate` is omitted, the wrapper resolves the current Japan date.

## Actions

- `Status`: inspect local readiness and existing artifacts; safe at any time.
- `Prepare`: run self-test and doctor. Use before the first raceday run after updates.
- `Morning`: collect same-day entries, schedule and JRA odds; validate input; create morning SHADOW analysis.
- `T5`: run the long-lived T-5 SHADOW runtime. It waits for race times and makes final SHADOW decisions. This command may remain active for most of the racing day.
- `Results`: after racing has finished, collect results/payouts, settle T-5 SHADOW tickets, update history/training data, create performance report, and run acceptance.
- `Verify`: run the acceptance check again and report the acceptance JSON.

## Normal operating sequence

1. Start with `Status`.
2. If the environment was updated or readiness is uncertain, run `Prepare`.
3. Before racing starts, run `Morning`.
4. Confirm the morning artifacts exist. Do not continue if morning failed.
5. Start `T5` early enough to remain running for the day. Do not terminate it just because it is waiting.
6. After all races have finished, run `Results`.
7. Run `Verify` if needed. Success means the acceptance report has `ok: true`.

## Safety and error handling

- Treat `NO_BET` as a normal outcome, not an error.
- Treat `FAILED` or `MISSED` T-5 states as conditions requiring investigation.
- Never hide a failing command and never continue automatically after a prerequisite failure.
- Do not edit historical source workbooks.
- Do not delete `history.sqlite3` unless the user explicitly requests a reset and a backup is made first.
- Do not retrain during the active racing day unless the user explicitly requests it. Use the already approved trained model for T-5.
- Do not change stake/race limits as part of ordinary operation.
- Do not change code merely to make acceptance pass. Diagnose the actual cause.

## What to report to the user

Keep the user-facing report simple. State:

- the race date,
- the action executed,
- whether it succeeded,
- current readiness or next action,
- any `FAILED`/`MISSED` issue,
- acceptance status when available.

For errors, include the failing phase and the most relevant error text. Do not ask the user to debug code manually; investigate the V2 code and artifacts first.

## Beginner-friendly intent phrases

Interpret requests such as these as invitations to use this skill:

- `今日の競馬V2を進めて`
- `実開催日の処理をして`
- `今日の状態を確認して`
- `朝処理して`
- `T-5を開始して`
- `レース結果を反映して`
- `今日のacceptanceを確認して`

When the user's requested phase is clear, run that phase. When it is not clear, run `Status` first and use the artifacts/current time to identify the next safe phase. Do not guess past a failed prerequisite.
