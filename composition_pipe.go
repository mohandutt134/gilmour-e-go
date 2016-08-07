package gilmour

import "time"

type PipeComposition struct {
	composition
}

func (c *PipeComposition) Execute(m *Message) (resp *Response, err error) {
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

		if len(c.executables()) > 0 && resp.Code() == 200 && err == nil {
			resp = inflateResponse(resp)
			do(do, resp.Next())
			return
		}
	}

	do(do, copyMessage(m))
	return
}

//New Pipe composition
func (g *Gilmour) NewPipe(cmds ...Executable) *PipeComposition {
	c := new(PipeComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}
