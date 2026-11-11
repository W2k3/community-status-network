Failure test (leader SIGKILL under load), 2026-11-11 12:04, heartbeat 1s, dead after 3s

| Trial | Election (s) | Write outage (s) | User write latency (s) | Read failures | Lost acked writes | Restart: serving (s) | Restart: caught up (s) | Entries behind | WALs identical |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 1 | 3.35 | 3.35 | 3.4 | 0 | 0/1304 | 0.08 | 0.13 | 963 | yes |
| 2 | 3.31 | 3.3 | 5.07 | 0 | 0/1380 | 0.07 | 0.13 | 1202 | yes |
| 3 | 3.3 | 3.35 | 3.43 | 0 | 0/1298 | 0.08 | 0.13 | 1022 | yes |
| **mean** | 3.32 | 3.33 | 3.97 | | | 0.08 | 0.13 | | |
