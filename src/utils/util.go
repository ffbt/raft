package utils

import "time"

func Max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func GetShortTime() time.Duration {
	return 10 * time.Millisecond
}

func GetMiddleTime() time.Duration {
	return 1000 * time.Millisecond
}

func ShortSleep() {
	time.Sleep(GetShortTime())
}
