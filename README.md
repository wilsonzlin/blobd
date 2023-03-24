# blobd

- Scales to millions of random concurrent partial reads over trillions of small objects with constant low latency.
- Strong data integrity with API-level guaranteed durability and replayable journaling.
- Asynchronous replication and event streaming.
- Fast direct I/O and TCP protocol.

## Notes

Interesting trivia: this project used to be called **Turbostore**, and was written entirely in C; you can still see the code [here](https://github.com/wilsonzlin/blobd/tree/ffb637ae4e4e91602ec04cf2fb2b50aafa116876).
