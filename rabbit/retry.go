package rabbit

import (
	"log"
	"math"
	"time"
)

type function func() error

func exponential(operation function, maxRetries int) error {
	var err error
	var sleepTime int
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		if i == 0 {
			sleepTime = 1
		} else {
			sleepTime = int(math.Exp2(float64(i)) * 100)
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		log.Printf("Retry exponential: Attempt %d, sleep %d", i, sleepTime)
	}

	return err
}
