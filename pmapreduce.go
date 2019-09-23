package pmapreduce

import (
	"gopkg.in/yaml.v3"
	"log"
)

var PMapperRepo map[string]func(*interface{}) *interface{}
var ReducerRepo map[string]func([]*interface{}) *interface{}

type pipelineDefWrapper struct {
	pipelineDef []struct {
		pipelineDefEl struct {
			pmappers []string `yaml:"pmappers"`
			reducer  string   `yaml:"reducer"`
		} `yaml:"pipelineDef-el"`
	} `yaml:"pipelineDef"`
}

type pipeline []struct {
	pmappers []func(*interface{}) *interface{}
	reducer  func([]*interface{}) *interface{}
}

func First(in []*interface{}) *interface{} {
	return in[0]
}

func prepPipeline(pYaml string) pipeline {
	unmarshalled := pipelineDefWrapper{}
	err := yaml.Unmarshal([]byte(pYaml), &unmarshalled)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	var p pipeline

	for index, pEl := range unmarshalled.pipelineDef {
		pmappers := make([]func(*interface{}) *interface{}, len(pEl.pipelineDefEl.pmappers),
			len(pEl.pipelineDefEl.pmappers))
		for _, pmapperId := range pEl.pipelineDefEl.pmappers {
			if pmapper, ok := PMapperRepo[pmapperId]; ok {
				pmappers = append(pmappers, pmapper)
			} else {
				log.Fatalf("mapped id=" + pmapperId + " isn't registered!")
			}
		}
		reducer, ok := ReducerRepo[pEl.pipelineDefEl.reducer]
		if !ok {
			log.Fatalf("reducer id=" + pEl.pipelineDefEl.reducer + " isn't registered!")
		}
		p[index] = struct {
			pmappers []func(*interface{}) *interface{}
			reducer  func([]*interface{}) *interface{}
		}{
			pmappers,
			reducer,
		}
	}
	return p
}
func RunPipeline(pYaml string, in *interface{}) *interface{} {
	p := prepPipeline(pYaml)

	result := in
	for _, el := range p {
		channels := make([]chan(*interface{}), len(el.pmappers), len(el.pmappers))
		for i, mapper := range el.pmappers {
			channels[i] = make(chan(*interface{}))
			go func(){
				channels[i] <- mapper(result)
			}()
		}
		mapped := make([]*interface{}, len(el.pmappers), len(el.pmappers))
		for i, ch := range channels {
			mapped[i] = <- ch
		}
		result = el.reducer(mapped)
	}
	return result
}
