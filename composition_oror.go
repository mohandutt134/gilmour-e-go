package gilmour

import "time"

func (g *Gilmour) NewOrOr(cmds ...Executable) *OrOrComposition {
	c := new(OrOrComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}

type OrOrComposition struct {
	composition
}

func (c *OrOrComposition) Execute(m *Message) (resp *Response, err error) {
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

		// Keep going, If the Pipeline has failed so far and there are still
		// some executables left.
		if len(c.executables()) > 0 && (resp.Code() > 200 || err != nil) {
			do(do, m)
			return
		}
	}

	do(do, m)
	return
}
