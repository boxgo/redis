package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/boxgo/box/minibox"
	"github.com/boxgo/metrics"
	"github.com/go-redis/redis/v7"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	// Redis config
	Redis struct {
		Metrics      bool     `config:"metrics" help:"default is false"`
		MasterName   string   `config:"masterName" help:"The sentinel master name. Only failover clients."`
		Address      []string `config:"address" help:"Either a single address or a seed list of host:port addresses of cluster/sentinel nodes."`
		Password     string   `config:"password" help:"Redis password"`
		DB           int      `config:"db" help:"Database to be selected after connecting to the server. Only single-node and failover clients."`
		PoolSize     int      `config:"poolSize" help:"Connection pool size"`
		MinIdleConns int      `config:"minIdleConns" help:"min idle connections"`

		name string
		redis.UniversalClient
		metrics *metrics.Metrics
		summary *prometheus.SummaryVec
		total   *prometheus.CounterVec
	}
)

const (
	start = "start"
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
		r.UniversalClient.AddHook(r)
		r.summary = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: r.metrics.Namespace,
				Subsystem: r.metrics.Subsystem,
				Name:      "redis_command",
				Help:      "redis command elapsed summary",
			},
			[]string{"address", "db", "masterName", "pipe", "cmd", "error"},
		)
		r.total = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: r.metrics.Namespace,
				Subsystem: r.metrics.Subsystem,
				Name:      "redis_command_total",
				Help:      "redis command total",
			},
			[]string{"address", "db", "masterName", "pipe", "cmd", "error"},
		)

		prometheus.MustRegister(r.summary, r.total)
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

func (r *Redis) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, start, time.Now()), nil
}

func (r *Redis) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	start := ctx.Value(start).(time.Time)
	elapsed := time.Now().Sub(start)

	r.report(false, elapsed, cmd)

	return nil
}

func (r *Redis) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, start, time.Now()), nil
}

func (r *Redis) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	start := ctx.Value(start).(time.Time)
	elapsed := time.Now().Sub(start)

	r.report(true, elapsed, cmds...)

	return nil
}

func (r *Redis) report(pipe bool, elapsed time.Duration, cmds ...redis.Cmder) {
	addressStr := strings.Join(r.Address, ",")
	dbStr := fmt.Sprintf("%d", r.DB)
	masterNameStr := r.MasterName
	errStr := ""
	cmdStr := ""
	pipeStr := fmt.Sprintf("%t", pipe)

	for _, cmd := range cmds {
		cmdStr += cmd.Name() + ";"

		if err := cmd.Err(); err != nil && err != redis.Nil {
			errStr += err.Error() + ";"
		}
	}
	cmdStr = strings.TrimSuffix(cmdStr, ";")

	values := []string{
		addressStr,
		dbStr,
		masterNameStr,
		pipeStr,
		cmdStr,
		errStr,
	}

	r.summary.WithLabelValues(values...).Observe(elapsed.Seconds())
	r.total.WithLabelValues(values...).Inc()
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
