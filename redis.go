package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/boxgo/box/minibox"
	"github.com/boxgo/metrics"
	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	// Redis config
	Redis struct {
		Metrics      bool     `config:"metrics" desc:"default is false"`
		MasterName   string   `config:"masterName" desc:"The sentinel master name. Only failover clients."`
		Address      []string `config:"address" desc:"Either a single address or a seed list of host:port addresses of cluster/sentinel nodes."`
		Password     string   `config:"password" desc:"Redis password"`
		DB           int      `config:"db" desc:"Database to be selected after connecting to the server. Only single-node and failover clients."`
		PoolSize     int      `config:"poolSize" desc:"Connection pool size"`
		MinIdleConns int      `config:"minIdleConns" desc:"min idle connections"`

		name string
		redis.UniversalClient
		metrics *metrics.Metrics
		counter *prometheus.CounterVec
	}
)

var (
	// Default redis
	Default = New("redis")
)

// Name config prefix
func (r *Redis) Name() string {
	return r.name
}

// Exts app
func (r *Redis) Exts() []minibox.MiniBox {
	return []minibox.MiniBox{r.metrics}
}

// ConfigWillLoad config will load
func (r *Redis) ConfigWillLoad(context.Context) {

}

// ConfigDidLoad config did load
func (r *Redis) ConfigDidLoad(context.Context) {
	if len(r.Address) == 0 || r.name == "" {
		panic("config is invalid: address and name is required")
	}

	r.UniversalClient = redis.NewUniversalClient(&redis.UniversalOptions{
		MasterName:   r.MasterName,
		Addrs:        r.Address,
		Password:     r.Password,
		DB:           r.DB,
		PoolSize:     r.PoolSize,
		MinIdleConns: r.MinIdleConns,
	})

	if r.Metrics {
		r.UniversalClient.WrapProcess(r.metricsProcess)
		r.UniversalClient.WrapProcessPipeline(r.metricsProcessPipeline)
		r.counter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: r.metrics.Namespace,
				Subsystem: r.metrics.Subsystem,
				Name:      "redis_command",
				Help:      "redis command counter",
			},
			[]string{"address", "db", "masterName", "pipe", "cmd", "error", "elpsed"},
		)

		prometheus.MustRegister(r.counter)
	}
}

// Serve start serve
func (r *Redis) Serve(ctx context.Context) error {
	_, err := r.Ping().Result()

	return err
}

// Shutdown close clients when Shutdown
func (r *Redis) Shutdown(ctx context.Context) error {
	if r.UniversalClient != nil {
		return r.Close()
	}

	return nil
}

func (r *Redis) metricsProcess(old func(redis.Cmder) error) func(redis.Cmder) error {
	return func(cmd redis.Cmder) error {
		start := time.Now()
		err := old(cmd)
		elpsed := time.Now().Sub(start)

		r.count(false, err, elpsed, cmd)

		return err
	}
}

func (r *Redis) metricsProcessPipeline(old func([]redis.Cmder) error) func([]redis.Cmder) error {
	return func(cmds []redis.Cmder) error {
		start := time.Now()
		err := old(cmds)
		elpsed := time.Now().Sub(start)

		r.count(true, err, elpsed, cmds...)

		return err
	}
}

func (r *Redis) count(pipe bool, err error, elpsed time.Duration, cmds ...redis.Cmder) {
	addressStr := strings.Join(r.Address, ",")
	dbStr := fmt.Sprintf("%d", r.DB)
	masterNameStr := r.MasterName
	errStr := ""
	cmdStr := ""
	eplsedStr := fmt.Sprintf("%f", elpsed.Seconds())
	pipeStr := fmt.Sprintf("%t", pipe)

	if err != nil && err != redis.Nil {
		errStr = err.Error()
	}

	for _, cmd := range cmds {
		cmdStr += cmd.Name() + ";"
	}
	cmdStr = strings.TrimSuffix(cmdStr, ";")

	values := []string{
		addressStr,
		dbStr,
		masterNameStr,
		pipeStr,
		cmdStr,
		errStr,
		eplsedStr,
	}

	r.counter.WithLabelValues(values...).Inc()
}

// New a redis
func New(name string, ms ...*metrics.Metrics) *Redis {
	if len(ms) == 0 {
		return &Redis{
			name:    name,
			metrics: metrics.Default,
		}
	}

	return &Redis{
		name:    name,
		metrics: ms[0],
	}
}
