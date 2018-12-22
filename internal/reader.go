package internal

type Reader interface {
	Buffered() int

	Bytes() []byte
	Read([]byte) (int, error)
	ReadByte() (byte, error)
	UnreadByte() error
	ReadSlice(byte) ([]byte, error)
	Discard(int) (int, error)

	//ReadBytes(fn func(byte) bool) ([]byte, error)
	//ReadN(int) ([]byte, error)
	ReadFull() ([]byte, error)
	ReadFullTemp() ([]byte, error)
}
