package types

import (
	"encoding/json"
	"fmt"

	"github.com/dustin/go-humanize"
)

type Bytes uint64

func (b Bytes) MarshalText() ([]byte, error) {
	return []byte(b.String()), nil
}

func (b *Bytes) UnmarshalText(data []byte) error {
	return b.Set(string(data))
}

func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.String())
}

func (b *Bytes) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as a number first
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		*b = Bytes(uint64(num))
		return nil
	}
	
	// If that fails, try as a string
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	return b.Set(raw)
}

func (b Bytes) String() string {
	return humanize.Bytes(uint64(b))
}

func (b *Bytes) UnmarshalYAML(unmarshal func(any) error) error {
	var raw string
	if err := unmarshal(&raw); err != nil {
		return err
	}

	value, err := humanize.ParseBytes(raw)
	if err != nil {
		return fmt.Errorf("invalid byte string %q: %w", raw, err)
	}

	*b = Bytes(value)

	return nil
}

func (b Bytes) MarshalYAML() (any, error) {
	return b.String(), nil
}

func (b Bytes) Bytes() uint64 {
	return uint64(b)
}

func (b Bytes) Int64() int64 {
	return int64(b)
}

func (b Bytes) Int() int {
	return int(b)
}

func (b *Bytes) Set(value string) error {
	parsed, err := humanize.ParseBytes(value)
	if err != nil {
		return err
	}
	*b = Bytes(parsed)
	return nil
}
