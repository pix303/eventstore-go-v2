package utils

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
)

func EncodePayload(item any) (string, error) {
	var buffer bytes.Buffer
	encoded := gob.NewEncoder(&buffer)
	err := encoded.Encode(item)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buffer.Bytes()), nil
}

func DecodePayload[T any](data string) (*T, error) {
	var item T
	gobData, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return &item, err
	}
	buffer := bytes.Buffer{}
	buffer.Write(gobData)
	decoder := gob.NewDecoder(&buffer)
	err = decoder.Decode(&item)
	if err != nil {
		return nil, err
	}
	return &item, nil
}
