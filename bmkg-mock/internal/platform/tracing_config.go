package platform

import "time"

// Milliseconds reads a positive millisecond duration without integer overflow.
func Milliseconds(key string, fallback int) time.Duration {
	value := Int(key, fallback)
	if value < 1 || value > 2147483647 {
		panic(key + " must be a positive 32-bit millisecond value")
	}
	return time.Duration(value) * time.Millisecond
}

func traceExportSettings() (int, int, time.Duration) {
	queue := Int("OTEL_BSP_MAX_QUEUE_SIZE", 128)
	batch := Int("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 32)
	if queue < 1 || batch < 1 || batch > queue {
		panic("trace batch size must be positive and no larger than the queue")
	}
	return queue, batch, Milliseconds("OTEL_BSP_EXPORT_TIMEOUT", 2000)
}
