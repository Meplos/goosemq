package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

func Marshall(data Frame) ([]byte, error) {
	buff := new(bytes.Buffer)
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	binary.Write(buff, binary.BigEndian, uint32(len(payload)))
	buff.Write(payload)
	return buff.Bytes(), nil

}

func Unmarshall(data []byte, dst json.RawMessage) (int, error) {
	return 0, nil

}
