# Redis storage example

This example keeps the Redis dependency in the application module rather than
in teleflow. The adapter in `storage.go` implements `storage.Storage` and stores
teleflow's opaque byte values without interpreting them. CAS operations use
Redis Lua scripts.

Start Redis locally, set the bot token, and run the example:

```sh
docker run --rm -p 6379:6379 redis:8-alpine
export TOKEN=your-telegram-bot-token
export REDIS_ADDR=localhost:6379
go run .
```

The atomic adapter protects state shared by multiple bot processes.
