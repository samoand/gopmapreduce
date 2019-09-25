package pmapreduce

import (
	"gotest.tools/assert"
	"testing"
)

var data []byte

func initialize() {
	data = []byte(`
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
        reducer: product`)
	PMapperRepo = make(map[string]func(interface{}) interface{})
	ReducerRepo = make(map[string]func([]interface{}) interface{})
	PMapperRepo["sumMapper0"] = sumMapper0
	PMapperRepo["sumMapper1"] = sumMapper1
	PMapperRepo["sumMapper2"] = sumMapper2
	PMapperRepo["sumMapper3"] = sumMapper3
	ReducerRepo["sumOfMapped"] = sumOfMapped

	PMapperRepo["mul2"] = mul2
	PMapperRepo["mul4"] = mul4
	ReducerRepo["product"] = product
}

type PipelineIn struct {
	first int
	last  int
	parts int
}

func partialSeqSum(first, last, parts, partNum int) int {
	increment := (last - first) / parts
	subFirst := increment * partNum
	subLast := subFirst + increment
	total := 0
	for i := subFirst; i < subLast; i++ {
		total += i
	}
	return total
}

func sumMapper(aIn interface{}, mapperNum int) interface{} {
	pipelineIn := aIn.(PipelineIn)
	first := pipelineIn.first
	last := pipelineIn.last
	parts := pipelineIn.parts
	var result interface{}
	result = partialSeqSum(first, last, parts, mapperNum)
	return result
}

func sumMapper0(aIn interface{}) interface{} {
	return sumMapper(aIn, 0)
}

func sumMapper1(aIn interface{}) interface{} {
	return sumMapper(aIn, 1)
}

func sumMapper2(aIn interface{}) interface{} {
	return sumMapper(aIn, 2)
}

func sumMapper3(aIn interface{}) interface{} {
	return sumMapper(aIn, 3)
}

func sumOfMapped(aIn []interface{}) interface{} {
	total := 0
	for _, el := range aIn {
		total += el.(int)
	}
	var result interface{}
	result = total
	return result
}

func mul(aIn interface{}, multiplier int) interface{} {
	in := aIn.(int)
	var result interface{}
	result = in * multiplier
	return result
}

func mul2(aIn interface{}) interface{} {
	result := mul(aIn, 2)
	return &result // '*' to illustrate how the framework would work with pointers
}

func mul4(aIn interface{}) interface{} {
	result := mul(aIn, 4)
	return &result // '*' to illustrate how the framework would work with pointers
}

func product(aIn []interface{}) interface{} {
	p := 1
	for _, el := range aIn {
		// mappers used '*' to illustrate how it would work with pointers
		// dereferencing below
		dereferenced := * el.(*interface{})
		num := dereferenced.(int)
		p *= num
	}
	var result interface{}
	result = p
	return result
}

const LAST = 40000

func TestRunPipeline(t *testing.T) {
	initialize()

	pipelineInput := PipelineIn{
		first: 0,
		last:  LAST,
		parts: 4,
	}
	pipelineResult := RunPipeline(data, pipelineInput)
	// below is the simplified way of calculating same thing.
	// that's what the chain does, in essense.
	plainPrelim := partialSeqSum(0, LAST, 1, 0)
	plainResult :=  plainPrelim * plainPrelim * 8
	assert.Equal(t, pipelineResult.(int), plainResult)
}
