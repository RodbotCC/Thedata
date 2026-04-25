# Deal Intelligence

This layer compresses each lead into an operator-facing deal sheet: commercial state, service mix, venue/payment readiness, and the next move.

## Snapshot
- Lead deal sheets: `211`
- Deposit / contract queue: `15`
- Venue pending queue: `4`
- Quote risk queue: `23`
- Decision pending queue: `47`

## Readiness Buckets
- contracting_or_deposit: 15
- quote_outstanding: 20
- tasting_pending: 27
- budget_risk: 3
- active_pipeline: 42
- venue_conflict: 2
- booked_fulfillment_watch: 12
- venue_pending: 2
- decision_pending: 7
- won_pipeline_watch: 5
- suppressed: 76

## Key Files
- `operator_action_board.md`: top active leads ordered by commercial readiness
- `deposit_ready_queue.md`: leads closest to signing / paying
- `venue_pending_queue.md`: leads blocked on venue status or venue rules
- `quote_risk_queue.md`: active quote-stage leads with budget or scope pressure
- `decision_pending_queue.md`: leads waiting on spouse / family / legal / other parties
- `../normalized/lead_deal_sheets.csv`: machine-friendly one-row-per-lead deal sheet index
