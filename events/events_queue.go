package events

type eventsQueue []Event

func (e eventsQueue) Len() int { return len(e) }

func (e *eventsQueue) Push(x Event) {
	*e = append(*e, x)
}

func (e *eventsQueue) Pop() (item Event) {
	item, (*e)[0] = (*e)[0], nil
	*e = (*e)[1:]
	return item
}
