package oneonone

// EventMessage An event received on new messages
type EventMessage struct {
	Payload []byte
}

// NewEventMessage Creates a new Message event
func NewEventMessage(payload []byte) *EventMessage {
	return &EventMessage{
		Payload: payload,
	}
}
