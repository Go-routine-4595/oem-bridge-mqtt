package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	event_hub "github.com/Go-routine-4595/oem-bridge-mqtt/adapters/controller/event-hub"
	"github.com/Go-routine-4595/oem-bridge-mqtt/adapters/gateway/mqtt"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/Go-routine-4595/oem-bridge-mqtt/adapters/controller"
	papi "github.com/Go-routine-4595/oem-bridge-mqtt/adapters/controller/api"
	"github.com/Go-routine-4595/oem-bridge-mqtt/crypto-util"
	//event_hub "github.com/Go-routine-4595/oem-bridge-mqtt/adapters/gateway/event-hub"
	"github.com/Go-routine-4595/oem-bridge-mqtt/middleware"
	"github.com/Go-routine-4595/oem-bridge-mqtt/model"
	"github.com/Go-routine-4595/oem-bridge-mqtt/service"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	config  = "config2.yaml"
	version = 0.1
	seed    = "this is my test"
)

var CompileDate string

type TypeName struct {
	Connection string `yaml:"Connection"`
	Topic      string `yaml:"Topic"`
}

type Config struct {
	controller.ControllerConfig `yaml:"ControllerConfig"`
	event_hub.EventHubConfig    `yaml:"EventHubConfig"`
	TypeName                    `yaml:"Mqtt"`
	Duration                    int `yaml:"Duration"`
	LogLevel                    int `yaml:"LogLevel"`
	EncryptionFlag              int `yaml:"EncryptionFlag"`
}

func main() {
	var (
		conf   Config
		svc    model.IService
		gtw    service.ISendAlarm
		eh     event_hub.IEventHub
		api    *papi.Api
		wg     *sync.WaitGroup
		ctx    context.Context
		args   []string
		sig    chan os.Signal
		cancel context.CancelFunc
		err    error
	)
	args = os.Args

	fmt.Println("Starting oem-alarm-bridge v", version)
	fmt.Println(CompileDate)

	wg = &sync.WaitGroup{}

	if len(args) == 1 {
		conf = openConfigFile(config)
	} else {
		conf = openConfigFile(args[1])
	}

	if conf.EncryptionFlag == 1 {
		conf.EventHubConfig.Connection, err = decrypt(conf.EventHubConfig.Connection)
		if err != nil {
			log.Error().Err(err).Msg("Failed to decrypt")
			os.Exit(-1)
		}
	}

	// provide additional info for the confg/API
	conf.ControllerConfig.CompileDate = CompileDate
	conf.ControllerConfig.Version = fmt.Sprintf("%.2f", version)

	// additional information about the config for the API ingo
	conf.ControllerConfig.LogLevel = conf.LogLevel
	conf.ControllerConfig.EncryptionFlag = conf.EncryptionFlag
	conf.ControllerConfig.MqttConnection = conf.TypeName.Connection
	conf.ControllerConfig.MqttTopic = conf.TypeName.Topic

	// log level
	log.Logger.With().Str("instanceId", "myid").Logger()
	log.Info().Msg("a message")
	zerolog.SetGlobalLevel(zerolog.InfoLevel + zerolog.Level(conf.LogLevel))
	conf.ControllerConfig.LogLevel = conf.LogLevel
	conf.EventHubConfig.LogLevel = conf.LogLevel

	fmt.Printf("Log level: ")
	switch zerolog.InfoLevel + zerolog.Level(conf.LogLevel) {
	case 5:
		fmt.Println("panic")
		conf.ControllerConfig.LogLevelString = "Panic"
	case 4:
		fmt.Println("fatal")
		conf.ControllerConfig.LogLevelString = "Fatal"
	case 3:
		fmt.Println("error")
		conf.ControllerConfig.LogLevelString = "Error"
	case 2:
		fmt.Println("warning")
		conf.ControllerConfig.LogLevelString = "Warning"
	case 1:
		fmt.Println("info")
		conf.ControllerConfig.LogLevelString = "Info"
	case 0:
		fmt.Println("debug")
		conf.ControllerConfig.LogLevelString = "Debug"
	case -1:
		fmt.Println("trace")
		conf.ControllerConfig.LogLevelString = "Panic"
	}

	// duration of the service (exit after duration)
	if conf.Duration > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(conf.Duration)*time.Minute)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	// new gateway (display or eh)
	//gtw = display.NewDisplay()
	gtw = mqtt.NewMqtt(conf.TypeName.Connection, conf.TypeName.Topic, conf.LogLevel, ctx)

	// new service with simple display
	svc = service.NewService(gtw)

	// new middleware
	svc = middleware.NewLogger(conf.ControllerConfig, svc)

	eh, err = event_hub.NewEventHubLight(svc, conf.EventHubConfig)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create event hub")
		os.Exit(-1)
	}
	// start the controller
	eh.Start(ctx, wg)

	// new Api
	api = papi.NewApi(conf.ControllerConfig)

	// start the Api
	api.Start(ctx, wg)

	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()
	// give 500 ms grace period to flush all logs
	time.Sleep(500 * time.Millisecond)
	wg.Wait()

}

func decrypt(cipheredTextSting string) (string, error) {

	var (
		key          = []byte(seed)
		res          []byte
		iv           []byte
		cipheredText []byte
		err          error
		plaintText   string
	)

	key = crypto_util.GenerateKey(string(key))
	iv, err = hex.DecodeString(crypto_util.IV)
	if err != nil {
		return "", errors.Join(err, errors.New("Failed to decode the IV"))
	}

	cipheredText, err = hex.DecodeString(cipheredTextSting)
	if err != nil {
		return "", errors.Join(err, errors.New("Failed to encode the IV"))
	}

	res, err = crypto_util.DecryptAES256CBC(cipheredText, key, iv)
	if err != nil {
		return "", errors.Join(err, errors.New("Failed to decrypt"))
	}
	plaintText = string(res)
	plaintText = strings.TrimRightFunc(plaintText, func(r rune) bool {
		return unicode.IsControl(r)
	})

	return plaintText, nil
}

func openConfigFile(s string) Config {
	if s == "" {
		s = "config.yaml"
	}

	f, err := os.Open(s)
	if err != nil {
		processError(errors.Join(err, errors.New("open config.yaml file")))
	}
	defer f.Close()

	var config Config
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		processError(err)
	}
	return config

}

func processError(err error) {
	fmt.Println(err)
	os.Exit(2)
}
