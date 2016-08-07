package gilmour

import "time"

//New Batch composition
func (g *Gilmour) NewBatch(cmds ...Executable) *BatchComposition {
	c := new(BatchComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}

type BatchComposition struct {
	recordableComposition
}

func (c *BatchComposition) Execute(m *Message) (resp *Response, err error) {
	batchResp := c.makeResponse()

	do := func(do recfunc, m *Message) {
		cmd := c.lpop()

		err = try(func(attempt int) (bool, error) {
			var err error
			resp, err = performJob(cmd, m)
			if err != nil {
				time.Sleep(c.engine.retryConf.Frequency)
			}
			return attempt < c.engine.retryConf.retryLimit, err
		})

		// Inflate and record the output in a single response.
		if c.isRecorded() {
			r := inflateResponse(resp)
			batchResp.write(r.Next())
		}

		if len(c.executables()) > 0 {
			do(do, m)
		}
	}

	do(do, m)

	// Automatically return last executable's response or override with
	// recorded output.
	if c.isRecorded() {
		resp = batchResp
	}

	return
}
