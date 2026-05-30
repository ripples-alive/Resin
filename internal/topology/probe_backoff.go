package topology

import "time"

func ProbeFailureBackoffInterval(base time.Duration, failureCount int32) time.Duration {
	if base <= 0 || failureCount <= 0 {
		return base
	}
	exponent := failureCount
	if exponent > 5 {
		exponent = 5
	}
	multiplier := int64(1) << exponent
	return base * time.Duration(multiplier)
}
