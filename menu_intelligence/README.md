# Menu Intelligence

This layer compresses buyer food preferences into a structured menu profile: cuisine style, item requests, dietary notes, venue food constraints, and the open menu questions still blocking movement.

## Snapshot
- Lead menu profiles: `211`
- Menu customization rows: `88`
- Venue food restriction rows: `14`
- Dietary watch rows: `4`
- Bar-program rows: `15`
- Cuisine-preference rows: `24`

## Key Files
- `menu_customization_board.md`: leads with dense menu questions or item requests
- `venue_food_restriction_board.md`: leads where kitchen / venue / staffing rules affect the menu
- `dietary_watch_board.md`: leads with dietary or allergy notes
- `bar_program_board.md`: leads with bar-direction asks or constraints
- `cuisine_preference_board.md`: leads with explicit cuisine-style direction
- `menu_signal_rollup.md`: global rollup across cuisines, topics, items, dietary flags, and venue constraints
- `../normalized/lead_menu_profiles.csv`: machine-friendly one-row-per-lead menu profile
