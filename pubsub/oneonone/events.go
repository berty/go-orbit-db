package oneonone

type EventMessage struct {
	Payload []byte
}

func NewEventMessage(payload []byte) *EventMessage {
	return &EventMessage{
		Payload: payload,
	}
}
