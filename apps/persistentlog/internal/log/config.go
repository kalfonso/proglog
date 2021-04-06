package log

// Config represents the configuration of the log.
type Config struct {
	Segment Segment
}

// Segment contains configuration for a segment.
type Segment struct {
	MaxStoreBytes uint64 // the maximum number of bytes in the store file.
	MaxIndexBytes uint64 // the maximum number of bytes in the index file.
	InitialOffset uint64 // the initial offset in the log.
}
