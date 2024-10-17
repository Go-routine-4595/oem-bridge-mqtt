package service

import (
	"encoding/json"
	"errors"
	"github.com/Go-routine-4595/oem-bridge-mqtt/model"
	hw "github.com/Go-routine-4595/oem-sim-g/model"
	"github.com/rs/zerolog/log"
)

type ISendAlarm interface {
	SendAlarm(events []model.FCTSDataModel) error
}

type Service struct {
	gateway ISendAlarm
	storage model.IStorage
}

func NewService(g ISendAlarm, s model.IStorage) *Service {
	return &Service{
		gateway: g,
		storage: s,
	}
}

func (s *Service) TestAlarm(value []byte) error {
	return nil
}

func (s *Service) SendAlarm(value []byte) error {
	var (
		events   []model.FCTSDataModel
		hwEvents hw.Events
		err      error
	)

	err = json.Unmarshal(value, &hwEvents)
	if err != nil {
		log.Error().Err(err).Msg("error unmarshalling")
		return errors.Join(err, errors.New("service json unmarshal error"))
	}

	events = s.Translate(hwEvents)

	log.Trace().Str("value", string(value)).Msg("sending alarm")

	return s.gateway.SendAlarm(events)
}

func (s *Service) Translate(events hw.Events) []model.FCTSDataModel {
	var (
		FCTSevents []model.FCTSDataModel
		FCTSevent  model.FCTSDataModel
		hwEvent    hw.AssetEvent
		properties []map[string]interface{}
		sapid      string
	)
	for _, hwEvent = range events.AssetEvents {
		// HW data model has 3 properties we nee to map in FCTS data model
		properties = make([]map[string]interface{}, 3)

		// Initialize the map at index 1
		properties[0] = make(map[string]interface{})
		properties[0]["EventName"] = hwEvent.EventName
		// Initialize the map at index 2
		properties[1] = make(map[string]interface{})
		properties[1]["CreatedUser"] = hwEvent.CreatedUser
		// Initialize the map at index 3
		properties[2] = make(map[string]interface{})
		properties[2]["AssetName"] = hwEvent.AssetName
		// Get the SAP id if any
		sapid = s.storage.GetSapId(hwEvent.AssetName)
		if sapid != "" {
			properties = append(properties, make(map[string]interface{}))
			properties[3]["SAP_ID"] = sapid
		}

		FCTSevent = model.FCTSDataModel{
			SiteCode:   "NAMEM",
			TimeStamp:  hwEvent.Timestamp.Unix(),
			SensorId:   "UAS-OEM-alarms",
			Uom:        "alarm",
			DataSource: "Honeywell",
			Value:      hwEvent.EventStatus,
			Annotation: model.Annotations{Properties: properties},
		}
		FCTSevents = append(FCTSevents, FCTSevent)
	}

	return FCTSevents
}
