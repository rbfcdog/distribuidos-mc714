package domain

type EventType int

const (
	Arrival EventType = iota
	Departure
)

type Request struct {
	ID          int
	ArrivalTime float64
	ServiceTime float64
}

type Event struct {
	Time     float64
	Type     EventType
	Req      *Request
	ServerID int
}

type Server struct {
	ID          int
	Queue       []*Request
	IsBusy      bool
	TotalServed int
}
