module github.com/hellocashmere/teleflow/examples/redis

go 1.27.1

require (
	github.com/hellocashmere/teleflow v0.0.0
	github.com/redis/go-redis/v9 v9.21.0
	gopkg.in/telebot.v4 v4.0.0-beta.5
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/hellocashmere/teleflow => ../..
