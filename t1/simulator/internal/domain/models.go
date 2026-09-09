// Package domain contains immutable values shared by the simulator.
package domain

// Request is a unit of work accepted by the load balancer.
type Request struct {
	ID          int
	ArrivalTime float64
}

// ServerSample captures a server's observable state immediately after an event.
type ServerSample struct {
	Time        float64
	ServerID    int
	Active      int
	QueueLength int
	Completed   int
}
