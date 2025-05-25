package types

import "fmt"

type Scheme string

const (
	SchemeHTTP  Scheme = "http"
	SchemeHTTPS Scheme = "https"
)

type Address struct {
	Host   string
	Port   int
	Scheme Scheme
}

type AddressOption func(*Address)

func WithScheme(scheme Scheme) AddressOption {
	return func(a *Address) {
		a.Scheme = scheme
	}
}

func NewAddress(host string, port int, opts ...AddressOption) Address {
	a := Address{
		Host:   host,
		Port:   port,
		Scheme: SchemeHTTP,
	}
	for _, opt := range opts {
		opt(&a)
	}
	return a
}

func (a Address) HostPort() string {
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

func (a Address) URL() string {
	return fmt.Sprintf("%s://%s:%d", a.Scheme, a.Host, a.Port)
}
