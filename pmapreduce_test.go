package pmapreduce

import (
	"gotest.tools/assert"
	"testing"
)

var data string

func initialize() {
	data = `
---
pipeline:
  - pipeline-el:
    pmappers:
      - sumMapper0
      - sumMapper1
      - sumMapper2
      - sumMapper3
	reducer: sumOfMapped
  - pipeline-el:
    pmappers:
      - mul2
      - mul4
    reducer: product
`
	PMapperRepo["sumMapper0"] = sumMapper0
	PMapperRepo["sumMapper1"] = sumMapper1
	PMapperRepo["sumMapper2"] = sumMapper2
	PMapperRepo["sumMapper3"] = sumMapper3
	ReducerRepo["sumOfMapped"] = sumOfMapped


	PMapperRepo["sumMapper3"] = sumMapper3
}

type PipelineIn struct {
	first  int
	last int
	parts int
}

func partialSeqSum(first, last, parts, partNum int) int {
	increment := (last - first)/parts
	subFirst := increment * partNum
	subLast := subFirst + increment
	total := 0
	for i := subFirst; i < subLast; i++ {
		total += i
	}
	return total
}

func sumMapper(aIn *interface{}, mapperNum int) *interface{} {
	pipelineIn := (*aIn).(PipelineIn)
	first := pipelineIn.first
	last := pipelineIn.last
	parts := pipelineIn.parts
	var result interface{}
	result = partialSeqSum(first, last, parts, mapperNum)
	return &result
}

func sumMapper0(aIn *interface{}) *interface{} {
	return sumMapper(aIn, 0)
}

func sumMapper1(aIn *interface{}) *interface{} {
	return sumMapper(aIn, 1)
}

func sumMapper2(aIn *interface{}) *interface{} {
	return sumMapper(aIn, 2)
}

func sumMapper3(aIn *interface{}) *interface{} {
	return sumMapper(aIn, 3)
}

func sumOfMapped(aIn []*interface{}) *interface{} {
	total := 0
	for _, el := range aIn {
		total += (*el).(int)
	}
	var result interface{}
	result = total
	return &result
}


func mul(aIn *interface{}, multiplier int) *interface{} {
	in := (*aIn).(int)
	var result interface{}
	result = in * multiplier
	return &result
}

func mul2(aIn *interface{}) *interface{} {
	return mul(aIn, 2)
}

func mul4(aIn *interface{}) *interface{} {
	return mul(aIn, 4)
}

func product(aIn  []*interface{}) *interface{} {
	p := 1
	for _, el := range aIn {
		num := (*el).(int)
		p *= num
	}
	var result interface{}
	result = p
	return &result
}

func TestRunPipeline(t *testing.T) {
	initialize()

	pipeline := PipelineIn{
		first: 0,
		last: 4000000,
		parts: 4,
	}
	var in interface{}
	in = pipeline
	pipelineResult := RunPipeline(data, &in)
	plainResult := partialSeqSum(0, 4000000, 1, 0) * 8
	assert.Equal(t, pipelineResult, plainResult)
}
