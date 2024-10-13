package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Go-routine-4595/oem-bridge-mqtt/model"
	pmqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
	uuid "github.com/satori/go.uuid"
	"os"
	"time"
)

type Mqtt struct {
	Topic    string
	MgtUrl   string
	logger   zerolog.Logger
	opt      *pmqtt.ClientOptions
	ClientID uuid.UUID
	client   pmqtt.Client
}

func NewMqtt(con string, topic string, logl int, ctx context.Context) *Mqtt {
	var (
		err error
		l   zerolog.Logger
		cid uuid.UUID
		//opt *pmqtt.ClientOptions
	)

	l = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(logl)).With().Timestamp().Int("pid", os.Getpid()).Logger()
	cid = uuid.NewV4()
	c := &Mqtt{
		Topic:    topic,
		MgtUrl:   con,
		logger:   l,
		ClientID: cid,
		opt: pmqtt.NewClientOptions().
			AddBroker(con).
			SetClientID("oem-alarm-bridge-" + cid.String()).
			SetCleanSession(true).
			SetAutoReconnect(true).
			SetTLSConfig(&tls.Config{
				InsecureSkipVerify: true,
			}).
			SetConnectionLostHandler(ConnectLostHandler(l)).
			SetOnConnectHandler(ConnectHandler(l)),
	}

	//opt.AddBroker("ssl://broker.emqx.io:8883")

	//c.opt.AddBroker("tcp://broker.hivemq.com:1883")

	go func() {
		<-ctx.Done()
		c.client.Disconnect(250)
		c.logger.Warn().Msg("Mqtt disconnect")
	}()

	err = c.Connect()
	if err != nil {
		panic(err)
	}

	return c
}

func (m *Mqtt) SendAlarm(events []model.FCTSDataModel) error {
	var (
		err   error
		b     []byte
		event model.FCTSDataModel
		token pmqtt.Token
	)

	for _, event = range events {
		b, err = json.Marshal(event)
		if err != nil {
			m.logger.Error().Err(err).Str("event", fmt.Sprintf("%v", event)).Msg("failed to marshal event")
			continue
		}
		token = m.client.Publish(m.Topic, 1, false, b)
		if token.WaitTimeout(200*time.Millisecond) && token.Error() != nil {
			m.logger.Error().Err(token.Error()).Str("event", fmt.Sprintf("%v", event)).Msg("Timeout exceeded during publishing")
		}
	}
	return nil
}

func (m *Mqtt) Disconnect() {
	m.client.Disconnect(500)
	m.logger.Info().Msg("Disconnected from mqtt broker")
	m.client = nil
}

func (m *Mqtt) Connect() error {
	m.client = pmqtt.NewClient(m.opt)
	if token := m.client.Connect(); token.Wait() && token.Error() != nil {
		m.logger.Error().Err(token.Error()).Msg("Error connecting to mqtt broker")
		return errors.Join(token.Error(), errors.New("Error connecting to mqtt broker"))
	}
	return nil
}

func (m *Mqtt) ConnectHandler() func(client pmqtt.Client) {
	return func(client pmqtt.Client) {
		m.logger.Info().Msg("Connected to mqtt broker")
	}
}

func (m *Mqtt) ConnectLostHandler() func(client pmqtt.Client, err error) {
	return func(client pmqtt.Client, err error) {
		m.logger.Warn().Err(err).Msg("Connection Lost")
	}
}

func ConnectHandler(logger zerolog.Logger) func(client pmqtt.Client) {
	return func(client pmqtt.Client) {
		logger.Info().Msg("Connected to mqtt broker")
	}
}

func ConnectLostHandler(logger zerolog.Logger) func(client pmqtt.Client, err error) {
	return func(client pmqtt.Client, err error) {
		logger.Warn().Err(err).Msg("Connection Lost")
	}
}
