package pincodeutils

func DeleteEmpty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

func GetConcurrency(workLength int) int {
	concurrency := workLength / BatchSize
	if remainder := workLength % BatchSize; remainder != 0 {
		concurrency++
	}
	return concurrency
}

func GetPartitionIndexes(workLength int, partition int) (startIndex int, endIndex int) {
	startIndex = (partition * BatchSize) + 1
	endIndex = (startIndex + BatchSize)
	if endIndex > workLength {
		endIndex = workLength
	}
	return
}
