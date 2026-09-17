package domain

type Request struct {
	ID          int
	ArrivalTime float64
}

type ServerSample struct {
	Time        float64
	ServerID    int
	Active      int
	QueueLength int
	Completed   int
	Capacity    int
	Backup      bool
}
