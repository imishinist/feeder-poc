package internal

type QueueMessage[T any] struct {
	ReceiptHandle string
	Message       T
}
