package gilmour

import (
	"log"
	"testing"
)

const (
	topicOne    = "compose-one"
	topicTwo    = "compose-two"
	topicThree  = "compose-three"
	topicBadTwo = "compose-bad-two"
)

type StrMap map[string]interface{}

func Setup(e *Gilmour) {
	o := MakeHandlerOpts()

	e.ReplyTo(topicBadTwo, func(r *Request, s *Message) {
		panic("bad-two")
	}, o)

	e.ReplyTo(topicOne, func(r *Request, s *Message) {
		req := StrMap{}
		r.Data(&req)
		compositionMerge(&req, &StrMap{"ack-one": "one"})
		s.Send(req)
	}, o)

	e.ReplyTo(topicTwo, func(r *Request, s *Message) {
		req := StrMap{}
		r.Data(&req)
		compositionMerge(&req, &StrMap{"ack-two": "two"})
		s.Send(req)
	}, o)

	e.ReplyTo(topicThree, func(r *Request, s *Message) {
		req := StrMap{}
		r.Data(&req)
		compositionMerge(&req, &StrMap{"ack-three": "three"})
		s.Send(req)
	}, o)
}

func init() {
	attachSetup(Setup)
}

func makeMessage(data interface{}) *Message {
	m := new(Message)
	m.SetData(data)
	return m
}

// Basic Merger which can be created and exposes a Transform method.
func TestFuncComposition(t *testing.T) {
	key := "merge"
	data := StrMap{"arg": 1}
	c := NewFuncComposition(StrMap{key: 1})
	msg := <-c.Execute(makeMessage(data), engine)
	if msg.GetCode() != 200 {
		t.Error("Should not have raised Error")
	}

	if _, ok := data[key]; !ok {
		t.Error("Must have", key, "in final output")
	}
}

// Transformation should fail while merging string to StrMap
func TestFuncCompositionFail(t *testing.T) {
	data := "arg"
	c := NewFuncComposition(StrMap{"merge": 1})
	msg := <-c.Execute(makeMessage(data), engine)
	if msg.GetCode() == 500 {
		t.Error("Should have raised error")
	}
}

func TestCompositionExecute(t *testing.T) {

	data := StrMap{}
	c := NewRequestComposition(topicOne)

	m := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	m.Unmarshal(&data)

	for _, key := range []string{"input", "ack-one"} {
		if _, ok := data[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestCompositionMergeExecute(t *testing.T) {
	data := StrMap{}
	c := NewRequestComposition(topicOne).With(StrMap{"merge-one": 1})

	m := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	m.Unmarshal(&data)

	for _, key := range []string{"input", "ack-one", "merge-one"} {
		if _, ok := data[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

// Basic composition Test composing single command with single transformation.
// Output must contain the output of the micro service morphed with the
// transformation.
func TestComposePipe(t *testing.T) {
	c := NewPipe(NewRequestComposition(topicOne).With(StrMap{"merge": 1}))

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	if _, ok := expected["merge"]; !ok {
		t.Error("Must have merge in final output")
	}

	if _, ok := expected["ack-one"]; !ok {
		t.Error("Must have ack-one in final output")
	}
}

func TestComposeComplex(t *testing.T) {
	c := NewPipe(
		NewRequestComposition(topicOne).With(StrMap{"merge-one": 1}),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	for _, key := range []string{"input", "merge-one", "ack-one", "merge-two", "ack-two"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestComposeNested(t *testing.T) {
	c11 := NewRequestComposition(topicOne).With(StrMap{"merge-one": 1})

	c2 := NewPipe(
		NewFuncComposition(StrMap{"fake-two": 1}),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
	)

	c1 := NewPipe(c11, c2)

	msg := <-c1.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	for _, key := range []string{"input", "merge-one", "ack-one", "fake-two", "merge-two", "ack-two"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestComposeAndAnd(t *testing.T) {
	c2 := NewPipe(
		NewFuncComposition(StrMap{"fake-two": 1}),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
	)

	c1 := NewAndAnd(
		NewRequestComposition(topicOne).With(StrMap{"merge-one": 1}),
		c2,
	)

	msg := <-c1.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	for _, key := range []string{"merge", "ack-one"} {
		if _, ok := expected[key]; ok {
			t.Error("Must NOT have", key, "in final output")
		}
	}

	for _, key := range []string{"input", "fake-two", "ack-two"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestComposeAndAndFail(t *testing.T) {
	c := NewAndAnd(
		NewRequestComposition(topicOne),
		NewRequestComposition(topicBadTwo),
		NewRequestComposition(topicTwo),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)

	if msg.GetCode() != 500 {
		t.Error("Request should have failed")
	}

	if len(c.compositions()) != 1 {
		t.Error("Composition should have been left with more commands.")
	}
}

func TestComposeBatchPass(t *testing.T) {
	c := NewBatch(
		NewRequestComposition(topicOne).With(StrMap{"merge": 1}),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	for _, key := range []string{"merge", "ack-one"} {
		if _, ok := expected[key]; ok {
			t.Error("Must NOT have", key, "in final output")
		}
	}

	for _, key := range []string{"input", "merge-two", "ack-two"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestComposeBatchWontFail(t *testing.T) {
	c := NewBatch(
		NewRequestComposition(topicOne),
		NewRequestComposition(topicBadTwo),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	if msg.GetCode() != 200 {
		t.Error("Request should have passed")
	}

	if len(c.compositions()) != 0 {
		t.Error("Composition should not be left with any more commands.")
	}

	for _, key := range []string{"merge", "ack-one"} {
		if _, ok := expected[key]; ok {
			t.Error("Must NOT have", key, "in final output")
		}
	}

	for _, key := range []string{"input", "ack-two", "merge-two"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}

func TestBatchRecordOutput(t *testing.T) {
	c := NewBatch(
		NewRequestComposition(topicOne),
		NewRequestComposition(topicTwo),
		NewRequestComposition(topicThree),
	)
	c.RecordOutput()

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)

	out := []*Message{}
	msg.Unmarshal(&out)

	if len(out) != 3 {
		t.Error("Must have captured 3 outputs.")
	}

	log.Println(msg.data)

	for _, m := range out {
		expected := StrMap{}
		if err := m.Unmarshal(&expected); err != nil {
			t.Error("Must be valid message output")
		} else if _, ok := expected["input"]; !ok {
			log.Println(m.data)
			t.Error("Must have input in final output")
		}
	}
}

/*
func TestBatchBadRecord(t *testing.T) {
	c := NewBatch().RecordOutput()

	c.Add(NewRequestComposition(topicOne))
	c.Add(NewRequestComposition(topicBadTwo))
	c.Add(NewRequestComposition(topicThree))

	c.Execute(makeMessage(StrMap{"input": 1}), engine)

	if len(c.GetOutput()) != 3 {
		t.Error("Must have captured 3 outputs.")
	}

	second := c.GetOutput()[1]
	if second.GetCode() != 500 {
		t.Error("Should have failed with 500")
	}
}

func TestAndAndRecordOutput(t *testing.T) {
	c := NewAndAnd().RecordOutput()

	c.Add(NewRequestComposition(topicOne))
	c.Add(NewRequestComposition(topicTwo))
	c.Add(NewRequestComposition(topicThree))

	c.Execute(makeMessage(StrMap{"input": 1}), engine)

	log.Println(c.GetOutput())

	if len(c.GetOutput()) != 3 {
		t.Error("Must have captured 3 outputs.")
	}

	for _, m := range c.GetOutput() {
		expected := StrMap{}
		if err := m.Unmarshal(&expected); err != nil {
			t.Error("Must be valid message output")
		} else if _, ok := expected["input"]; !ok {
			t.Error("Must have input in final output")
		}
	}
}
*/

func TestOrOr(t *testing.T) {
	c := NewOrOr(
		NewRequestComposition(topicBadTwo),
		NewRequestComposition(topicOne),
		NewRequestComposition(topicThree),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)
	expected := StrMap{}
	msg.Unmarshal(&expected)

	for _, key := range []string{"ack-three"} {
		if _, ok := expected[key]; ok {
			t.Error("Must NOT have", key, "in final output")
		}
	}

	for _, key := range []string{"input", "ack-one"} {
		if _, ok := expected[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}

}

func TestParallel(t *testing.T) {
	c := NewParallel(
		NewRequestComposition(topicOne).With(StrMap{"merge-one": 1}),
		NewRequestComposition(topicTwo).With(StrMap{"merge-two": 1}),
		NewRequestComposition(topicThree).With(StrMap{"merge-three": 1}),
	)

	msg := <-c.Execute(makeMessage(StrMap{"input": 1}), engine)

	out := []*Message{}
	msg.Unmarshal(&out)

	if len(out) != 3 {
		t.Error("Must have captured 3 outputs.")
	}

	first := StrMap{}
	out[0].Unmarshal(&first)
	for _, key := range []string{"input", "ack-one", "merge-one"} {
		if _, ok := first[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}

	second := StrMap{}
	out[1].Unmarshal(&second)
	for _, key := range []string{"input", "ack-two", "merge-two"} {
		if _, ok := second[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}

	third := StrMap{}
	out[2].Unmarshal(&third)
	for _, key := range []string{"input", "ack-three", "merge-three"} {
		if _, ok := third[key]; !ok {
			t.Error("Must have", key, "in final output")
		}
	}
}
