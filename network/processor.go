package network

import "context"

type Processor interface {
	Process(ctx context.Context, m Message) error
	ReportUnreachable(id uint64)
}
