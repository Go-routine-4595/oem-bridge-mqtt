package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
)

type StorageConf struct {
	FileName         string `yaml:"FileName"`
	ConnectionString string `yaml:"ConnectionString"`
}

type Storage struct {
	db istorage
}

func NewStorage(conf StorageConf, ctx context.Context) *Storage {
	var (
		db  istorage
		err error
	)
	if conf.ConnectionString == "" {
		db, err = newFileStorage(conf.FileName)
		if err != nil {
			if errors.Is(err, err1) || errors.Is(err, err2) {
				// do stuff here
			}
		}
		return &Storage{
			db: db,
		}
	}
	if conf.ConnectionString != "" {
		// do stuff here
	}
	// we should not reach this
	panic("something bad happened")
}

func (s *Storage) GetSapId(a string) string {
	return s.db.GetSapId(a)
}

type istorage interface {
	GetSapId(a string) string
}

// ----------------------------------------
// ------ File Storage --------------------
// ----------------------------------------
var err1 error = errors.New("could not open the json file file")
var err2 error = errors.New("could not unmarshall the json file file")

type fileStorage struct {
	DB map[string]string
}

func newFileStorage(f string) (fileStorage, error) {
	var (
		db  map[string]string
		err error
	)

	db, err = openConfigFile(f)

	return fileStorage{
		DB: db,
	}, err
}

func (f fileStorage) GetSapId(a string) string {
	return f.DB[a]
}

func openConfigFile(s string) (map[string]string, error) {
	if s == "" {
		return map[string]string{}, errors.New("no file name provided")
	}

	f, err := os.Open(s)
	if err != nil {
		return map[string]string{}, errors.Join(err, errors.New("could not open the json file file"))
	}
	defer f.Close()

	var (
		config map[string]string
		b      []byte
	)

	config = make(map[string]string)
	b, err = io.ReadAll(f)
	if err != nil {
		return map[string]string{}, errors.Join(err, err1)
	}

	err = json.Unmarshal(b, &config)
	if err != nil {
		return map[string]string{}, errors.Join(err, err2)
	}
	return config, nil

}
