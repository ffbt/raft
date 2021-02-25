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

func ShortSleep() {
	time.Sleep(10 * time.Millisecond)
}
