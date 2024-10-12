package event_hub

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Go-routine-4595/oem-bridge-mqtt/model"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

// connection string can have the event hub name like this
// see https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-get-connection-string
// see https://azure.github.io/azure-sdk/golang_introduction.html
// see https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs#ConsumerClient.Close
// see https://github.com/Azure/azure-sdk-for-go?tab=readme-ov-file

const (
	batchSize = 50
)

type IEventHub interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
}

type EventHubConfig struct {
	Connection   string `yaml:"connection"`
	EventHubName string `yaml:"EventHubName"`
	LogLevel     int    `yaml:"LogLevel"`
}

type EventHub struct {
	consumerClient *azeventhubs.ConsumerClient
	processor      *azeventhubs.Processor
	logger         zerolog.Logger
	Svc            model.IService
}

func NewEventHub(svc model.IService, conf EventHubConfig) (IEventHub, error) {
	var (
		err             error
		consumerClient  *azeventhubs.ConsumerClient
		l               zerolog.Logger
		cfg             *tls.Config
		clientOptions   *azeventhubs.ConsumerClientOptions
		checkClient     *container.Client
		checkpointStore *checkpoints.BlobStore
		eh              *EventHub
	)
	l = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(conf.LogLevel)).With().Timestamp().Int("pid", os.Getpid()).Logger()

	cfg = &tls.Config{
		InsecureSkipVerify: true,
	}
	clientOptions = &azeventhubs.ConsumerClientOptions{
		ApplicationID: "oem-alarms",
		TLSConfig:     cfg,
	}

	consumerClient, err = azeventhubs.NewConsumerClientFromConnectionString(conf.Connection, conf.EventHubName, azeventhubs.DefaultConsumerGroup, clientOptions)

	eh = &EventHub{
		consumerClient: consumerClient,
		logger:         l,
		Svc:            svc,
	}

	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create producer client"))
	}

	// connection string
	// 'DefaultEndpointsProtocol=https;AccountName=oemalarmstorage;AccountKey=<accountKey>;EndpointSuffix=core.windows.net
	blobConnectionString := "" // coming form config file
	// create a container client using a connection string and container name
	checkClient, err = container.NewClientFromConnectionString(blobConnectionString, "oem-storage", nil)
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create a container"))
	}

	// create a checkpoint store that will be used by the event hub
	checkpointStore, err = checkpoints.NewBlobStore(checkClient, nil)
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create a checkpoint store"))
	}

	// create a processor to receive and process events
	eh.processor, err = azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		return nil, errors.Join(err, errors.New("failed to create a processor"))
	}

	//  for each partition in the event hub, create a partition client with processEvents as the function to process events
	dispatchPartitionClients := func() {
		for {
			partitionClient := eh.processor.NextPartitionClient(context.TODO())

			if partitionClient == nil {
				break
			}

			go func() {
				if err := eh.processEvents(partitionClient); err != nil {
					panic(err)
				}
			}()
		}
	}

	// run all partition clients
	go dispatchPartitionClients()

	l.Info().Msg("Event Hub handler created")

	return eh, nil
}

func (e *EventHub) Start(ctx context.Context, wg *sync.WaitGroup) {
	var (
		err error
	)

	go func() {
		wg.Add(1)

		processorCtx, processorCancel := context.WithCancel(ctx)
		defer processorCancel()
		defer e.consumerClient.Close(context.TODO())
		defer wg.Done()

		if err = e.processor.Run(processorCtx); err != nil {
			e.logger.Panic().Err(err).Msg("Event Hub handler failed to start or encountered an error")
		}
		e.logger.Info().Msg("Event Hub handler stopped")
	}()
}

func (e *EventHub) processEvents(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		e.logger.Debug().Int("count", len(events)).Msg("Event Hub handler received events")
		//fmt.Printf("Processing %d event(s)\n", len(events))

		for _, event := range events {
			//fmt.Printf("Event received with body %v\n", string(event.Body))
			e.logger.Debug().Str("event", string(event.Body)).Msg("Event Hub handler received event")
			err := e.Svc.SendAlarm(event.Body)
			if err != nil {
				e.logger.Error().Err(err).Msg("Event Hub handler failed to send alarm")
			}
		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				return err
			}
		}
	}
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}
