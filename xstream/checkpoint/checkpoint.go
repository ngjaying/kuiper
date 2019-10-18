package checkpoint

type Message int

const (
	STOP Message = iota
	ACK
	DEC
)

type Signal struct {
	Message      Message
	Barrier
}

type Barrier struct {
	CheckpointId int64
	OpId 		 string
}

