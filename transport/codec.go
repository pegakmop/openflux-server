package transport

import "fmt"

// WrapCodec applies the wire codec named by codec ("legacy" or "batched" -
// see the --codec flag in main.go) to inner, matching how ParseExitMode
// validates the --mode flag. Both ends of a tunnel must agree on this: a
// client and exit node disagreeing here can't decode each other's frames.
func WrapCodec(inner Transport, codec string) (Transport, error) {
	switch codec {
	case "", "legacy":
		return NewCompressedTransport(inner), nil
	case "batched":
		return NewBatchedTransport(inner), nil
	default:
		return nil, fmt.Errorf("unknown codec %q (want legacy|batched)", codec)
	}
}
