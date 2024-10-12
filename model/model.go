package model

type IService interface {
	SendAlarm(events []byte) error
	TestAlarm(events []byte) error
}

type FCTSDataModel struct {
	SiteCode   string      `json:"site_code"`
	SensorId   string      `json:"sensor_id"`
	DataSource string      `json:"data_source"`
	TimeStamp  int64       `json:"time_stamp"`
	Value      string      `json:"value"`
	Uom        string      `json:"uom"`
	Quality    string      `json:"quality"`
	Annotation Annotations `json:"annotations"`
}

type Annotations struct {
	Properties []map[string]interface{} `json:"properties"`
}
