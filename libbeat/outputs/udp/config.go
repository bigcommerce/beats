package udp

import (
	"errors"
	"time"
)

var (
	ErrNoHostsConfigured = errors.New("no hosts configured")
)

type config struct {
	Timeout            time.Duration            `config:"timeout"`
	Formats            map[string]*formatConfig `config:"formats" validate:"required"`
	Hosts              []string                 `config:"hosts" validate:"required"`
	Port               int                      `config:"port" validate:"required"`
	Backoff            backoff                  `config:"backoff"`
	BulkMaxSize        int                      `config:"bulk_max_size" default:"100"`
	MaxRetries         int                      `config:"max_retries" default:"3"`
	SkipExpiredEntries bool                     `config:"skip_expired_entries" default:"true"`
	ExpirationLimit    time.Duration            `config:"expiration_limit" default:"5m"`
}

type backoff struct {
	Init time.Duration `config:"init" default:"1s"`
	Max  time.Duration `config:"max" default:"60s"`
}

type formatConfig struct {
	Conditions         string `config:"conditions"`
	Format             string `config:"format" validate:"required"`
	ProcessURL         string `config:"processURL"`
	ProcessURLProperty string `config:"processURLProperty"`
	TimeProperty       string `config:"timeProperty"`
	TimeFormatUseEpoch bool   `config:"timeFormatUseEpoch"`
	TimeFormat         string `config:"timeFormat"`
}

var (
	defaultConfig = config{
		Backoff: backoff{
			Init: 10 * time.Second,
			Max:  3,
		},
	}
)

func (c *config) Validate() error {
	if len(c.Hosts) == 0 {
		return ErrNoHostsConfigured
	}
	return nil
}
