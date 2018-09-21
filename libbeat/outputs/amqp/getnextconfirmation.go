package amqp

import "github.com/streadway/amqp"

// getNextConfirmation polls both returns and confirmations to get the next
// confirmation from amqp.
func getNextConfirmation(rch <-chan amqp.Return, cch <-chan amqp.Confirmation) (*amqp.Confirmation, *amqp.Return, error) {
	var retPtr *amqp.Return
	var conPtr *amqp.Confirmation
	var err error

	// The amqp library will write returns to rch before writing an ack for a
	// returned message to cch.
	//
	// We rely on the ordering guarantees of the amqp library to match amqp
	// confirmations to our pending beat publishes, so we need to carefully
	// check both. This must be non-blocking since there could be signals on cch
	// but not on rch.
	//
	// However, when there _is_ a signal on rch, we need to guarantee we consume
	// the next signal on cch to pair a return and confirmation.

Select:
	select {
	case ret, ok := <-rch:
		if ok {
			retPtr = &ret

			// returns are meant to be immediately followed by an ack on the
			// confirmations channel from the amqp library
			if con, ok := <-cch; ok {
				conPtr = &con
			} else {
				// we're in an unstable state if cch is closed now as there's
				// both a pending publish and an orphaned return signal
				return nil, nil, ErrConfirmationsClosedWithReturn
			}
		} else {
			// we need the next confirmation to completely report a return
			rch = nil // ensure that `<-rch` yields to `<-cch` in our select{}
			goto Select
		}
	case con, ok := <-cch:
		if ok {
			conPtr = &con
		} else {
			// we're in an unstable state if cch is closed when we are looking
			// to confirm a pending publish
			err = ErrConfirmationsClosed
		}
	}

	return conPtr, retPtr, err
}
