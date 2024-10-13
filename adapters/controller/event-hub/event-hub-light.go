package event_hub

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Go-routine-4595/oem-bridge-mqtt/model"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/rs/zerolog"
)

type EventHubLight struct {
	consumerClient *azeventhubs.ConsumerClient
	logger         zerolog.Logger
	Svc            model.IService
}

func NewEventHubLight(svc model.IService, conf EventHubConfig) (IEventHub, error) {
	var (
		err           error
		ehl           *EventHubLight
		clientOptions *azeventhubs.ConsumerClientOptions
	)

	ehl = &EventHubLight{
		logger: zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel+zerolog.Level(conf.LogLevel)).With().Timestamp().Int("pid", os.Getpid()).Logger(),
		Svc:    svc,
	}

	clientOptions = &azeventhubs.ConsumerClientOptions{InstanceID: "oem-bridge"}

	// Create a client handler
	ehl.consumerClient, err = azeventhubs.NewConsumerClientFromConnectionString(conf.Connection, conf.EventHubName, azeventhubs.DefaultConsumerGroup, clientOptions)
	if err != nil {
		ehl.logger.Error().Err(err).Str("connection", conf.Connection).Msg("Failed to create event hub client")
		return nil, errors.Join(err, errors.New("Failed to create event hub client"))
	}
	return ehl, nil
}

func (ehl *EventHubLight) Start(ctx context.Context, wg *sync.WaitGroup) {
	var (
		eventHubProps azeventhubs.EventHubProperties
		err           error
	)

	wg.Add(1)
	// Get information about the Event Hub (like partition IDs)
	eventHubProps, err = ehl.consumerClient.GetEventHubProperties(ctx, nil)
	if err != nil {
		ehl.logger.Error().Err(err).Msg("Failed to get event hub properties")
		wg.Done()
		return
	}
	// Consume events from all partitions
	for _, partitionID := range eventHubProps.PartitionIDs {
		ehl.logger.Info().Str("partitionID", partitionID).Msg("Starting partition consumer")
		go ehl.consumePartition(ctx, partitionID)
	}
}

func (ehl *EventHubLight) consumePartition(ctx context.Context, partitionID string) {
	var (
		partitionClient *azeventhubs.PartitionClient
		events          []*azeventhubs.ReceivedEventData
		receiveCtx      context.Context
		cancelFunc      context.CancelFunc
		err             error
	)

	// Create a partition consumer for the specific partition
	partitionClient, err = ehl.consumerClient.NewPartitionClient(
		partitionID,
		&azeventhubs.PartitionClientOptions{
			// Start from the latest event in the stream
			StartPosition: azeventhubs.StartPosition{
				Earliest: to.Ptr(false), // Set to true if you want to start from the earliest event
			},
		})
	if err != nil {
		ehl.logger.Fatal().Err(err).Str("partitionID", partitionID).Msg("failed to create partition client")
	}
	defer partitionClient.Close(ctx)
	defer ehl.consumerClient.Close(ctx)

	// Start receiving events from the latest position
	for {
		// Receive events in batches (up to 10 messages at a time or wait for 10s maximum before flushing the batch
		receiveCtx, cancelFunc = context.WithTimeout(ctx, 10000*time.Millisecond)
		events, err = partitionClient.ReceiveEvents(receiveCtx, 10, nil)
		cancelFunc()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				ehl.logger.Warn().Err(err).Str("partitionID", partitionID).Msg("context canceled, shutting down")
				return
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				ehl.logger.Error().Err(err).Str("partitionID", partitionID).Msg("failed to receive events")
				return
			}
			continue
		}

		for _, event := range events {
			ehl.logger.Debug().Str("event", string(event.Body)).Str("partitionID", partitionID).Msg("Received event")
			ehl.Svc.SendAlarm(event.Body)
		}

	}
}
