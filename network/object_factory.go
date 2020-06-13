package network

func NewBatchMessage(value *Message, cap int) *BatchMessage {
	var batchMessage *BatchMessage
	batchMessage = &BatchMessage{}
	batchMessage.Term = value.Term
	batchMessage.Messages = make([]*Message, 0, cap)
	batchMessage.Messages = append(batchMessage.Messages, value)

	return batchMessage
}

func NewOnlyOneMsg(term uint64, key uint64, text string, msgType uint32) *BatchMessage {
	var batchMessage *BatchMessage
	var msg *Message
	msg = &Message{}
	msg.Term = term
	msg.Index = key
	msg.Text = text
	msg.Count = 1
	msg.Type = msgType
	batchMessage = &BatchMessage{}
	batchMessage.Term = msg.Term
	batchMessage.Messages = make([]*Message, 0, 1)
	batchMessage.Messages = append(batchMessage.Messages, msg)
	return batchMessage
}
