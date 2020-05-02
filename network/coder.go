package network

type encoder interface {
	// encode encodes the given message to an output stream.
	encode(m *Message) error
}

type decoder interface {
	// decode decodes the message from an input stream.
	decode() (Message, error)
}
