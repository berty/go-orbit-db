package operation

type Operation interface {
	GetKey() string
	GetOperation() string
	GetValue() []byte
	Marshal() ([]byte, error)
}
