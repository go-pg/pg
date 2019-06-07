package types

const (
	quoteFlag = 1 << iota
	arrayFlag
	subArrayFlag
)

func hasFlag(flags, flag int) bool {
	return flags&flag == flag
}

func shouldQuoteArray(flags int) bool {
	return hasFlag(flags, quoteFlag) && !hasFlag(flags, subArrayFlag)
}
