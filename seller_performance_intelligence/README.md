# Seller Performance Intelligence

This layer compresses seller-side execution into a working view: message response speed, promise-keeping, stall signals, and quote friction by owner and by lead.

## Snapshot
- Lead seller signal sheets: `211`
- Response turns: `536`
- Owner summaries: `6`
- Stalled active leads: `48`
- Quote-friction active leads: `12`

## Key Files
- `owner_performance_overview.md`: one-row-per-owner seller execution view
- `response_speed_board.md`: owner-level message response and unanswered-turn pressure
- `promise_followthrough_board.md`: owner-level promise-keeping pressure
- `stalled_lead_board.md`: active leads that are currently stuck
- `quote_friction_board.md`: active leads where commercial friction is slowing movement
- `../normalized/lead_seller_performance_signals.csv`: machine-friendly one-row-per-lead seller signal profile
- `../normalized/message_response_turns.csv`: machine-friendly inbound-to-outbound response turns
