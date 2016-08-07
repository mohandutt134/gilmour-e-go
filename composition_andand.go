package gilmour

import "time"

//New AndAnd composition.
func (g *Gilmour) NewAndAnd(cmds ...Executable) *AndAndComposition {
	c := new(AndAndComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}

type AndAndComposition struct {
	composition
}

func (c *AndAndComposition) Execute(m *Message) (resp *Response, err error) {
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

		// Keep going if nothing has failed so far.
		if len(c.executables()) > 0 && err == nil && resp.Code() == 200 {
			do(do, m)
			return
		}
	}

	do(do, m)
	return
}
