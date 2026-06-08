package inner

// Binary result framing used on broker hops that carry pre-encoded external
// protocol envelopes (so the gateway can forward them to the client verbatim
// without decoding the payload):
//
//	[16B client UUID][external envelope: 1B msgType + 4B payload len + payload]
const clientIDSize = 16
