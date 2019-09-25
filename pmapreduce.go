package pmapreduce

import (
	"gopkg.in/yaml.v3"
	"log"
)

var PMapperRepo map[string]func(interface{}) interface{}
var ReducerRepo map[string]func([]interface{}) interface{}

type PipelineDefWrapper struct {
	PipelineDef []struct {
		PipelineDefEl struct {
			Pmappers []string `yaml:"pmappers"`
			Reducer  string   `yaml:"reducer"`
		} `yaml:"pipeline-el"`
	} `yaml:"pipeline"`
}

type pipelineEl struct {
	pmappers []func(interface{}) interface{}
	reducer  func([]interface{}) interface{}
}
type pipeline []pipelineEl

func ReduceToFirst(in []interface{}) interface{} {
	return in[0]
}

func ReduceToNone(in []interface{}) interface{} {
	return nil
}

func MapperNoop(in interface{}) interface{} {
	return in
}

func prepPipeline(pYaml []byte) pipeline {
	unmarshalled := PipelineDefWrapper{}
	err := yaml.Unmarshal(pYaml, &unmarshalled)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	var p pipeline = make([]pipelineEl, len(unmarshalled.PipelineDef), len(unmarshalled.PipelineDef))

	for index, pEl := range unmarshalled.PipelineDef {
		pmappers := make([]func(interface{}) interface{}, len(pEl.PipelineDefEl.Pmappers),
			len(pEl.PipelineDefEl.Pmappers))
		for innerIndex, pmapperId := range pEl.PipelineDefEl.Pmappers {
			if pmapper, ok := PMapperRepo[pmapperId]; ok {
				pmappers[innerIndex] = pmapper
			} else {
				log.Fatalf("mapped id=" + pmapperId + " isn't registered!")
			}
		}
		reducer, ok := ReducerRepo[pEl.PipelineDefEl.Reducer]
		if !ok {
			log.Fatalf("Reducer id=" + pEl.PipelineDefEl.Reducer + " isn't registered!")
		}
		p[index] = struct {
			pmappers []func(interface{}) interface{}
			reducer  func([]interface{}) interface{}
		}{
			pmappers,
			reducer,
		}
	}
	return p
}
func RunPipeline(pYaml []byte, in interface{}) interface{} {
	p := prepPipeline(pYaml)

	result := in
	for _, el := range p {
		channels := make([]chan(interface{}), len(el.pmappers), len(el.pmappers))
		for i, mapper := range el.pmappers {
			channels[i] = make(chan interface{})
			go func(index int, mapper func(interface{})interface{}){
				mapperResult := mapper(result)

				channels[index] <- mapperResult
			}(i, mapper)
		}
		mapped := make([]interface{}, len(el.pmappers), len(el.pmappers))
		for i, ch := range channels {
			mapped[i] = <- ch
		}
		result = el.reducer(mapped)
	}
	return result
}
