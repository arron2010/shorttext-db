package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
)

// messageEncoder is a encoder that can encode all kinds of messages.
// It MUST be used with a paired messageDecoder.
type messageEncoder struct {
	w io.Writer
}

func (enc *messageEncoder) encode(m *Message) error {

	//if m.Type == raftpb.MsgPayload {
	//	fmt.Println("messageEncoder--->", m.String())
	//}

	if err := binary.Write(enc.w, binary.BigEndian, uint64(Size(m))); err != nil {
		return err
	}

	_, err := enc.w.Write(mustMarshal(m))
	return err
}

// messageDecoder is a decoder that can decode all kinds of messages.
type messageDecoder struct {
	r io.Reader
}

var (
	readBytesLimit     uint64 = 512 * 1024 * 1024 // 512 MB
	ErrExceedSizeLimit        = errors.New("rafthttp: error limit exceeded")
)

func (dec *messageDecoder) decode() (Message, error) {
	return dec.decodeLimit(readBytesLimit)
}

func (dec *messageDecoder) decodeLimit(numBytes uint64) (Message, error) {
	var m *Message = &Message{}
	var l uint64
	if err := binary.Read(dec.r, binary.BigEndian, &l); err != nil {
		return *m, err
	}
	if l > numBytes {
		return *m, ErrExceedSizeLimit
	}
	buf := make([]byte, int(l))
	if _, err := io.ReadFull(dec.r, buf); err != nil {
		return *m, err
	}
	err := unmarshal(m, buf)
	return *m, err
}

func mustMarshal(pb proto.Message) []byte {
	d, err := proto.Marshal(pb)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}
	return d
}

func unmarshal(pb proto.Message, data []byte) error {
	return proto.Unmarshal(data, pb)
}
