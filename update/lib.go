package update

import (
	"errors"

	"go.mongodb.org/mongo-driver/bson"
)

type Document = bson.D
type UpdateOperation = map[string]interface{}
type upT = map[string]interface{}

type updateStep interface {
	apply(current bson.D) (bson.D, error)
}

func UpdateDocument(document Document, operation UpdateOperation) (Document, error) {
	if len(operation) == 0 {
		return nil, errors.New("update document must have at least one element")
	}
	steps := []updateStep{}
	for k, v := range operation {
		step, err := parseOperationStep(k, v)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	newDoc := document
	var err error
	for _, step := range steps {
		newDoc, err = step.apply(newDoc)
		if err != nil {
			return nil, err
		}
	}
	return newDoc, nil
}

func parseOperationStep(k string, v interface{}) (updateStep, error) {
	switch k {
	case "$set":
		return newSet(v)
	default:
		return nil, errors.New("fuck")
	}
}

type setDoc = map[string]interface{}
type setOperation struct {
	doc setDoc
}

func newSet(doc interface{}) (*setOperation, error) {
	asType, ok := doc.(setDoc)
	if !ok {
		return nil, errors.New("set operation invalid type")
	}
	return &setOperation{
		doc: asType,
	}, nil
}
func (o *setOperation) apply(current bson.D) (bson.D, error) {
	// set does an upsert. This does the insert part
	unvisitedNodes := map[string]bool{}
	for v, _ := range o.doc {
		unvisitedNodes[v] = true
	}
	newDoc := bson.D{}
	for _, v := range current {
		val, ok := o.doc[v.Key]
		if ok {
			delete(unvisitedNodes, v.Key)
			newDoc = append(newDoc, bson.E{v.Key, val})
		} else {
			newDoc = append(newDoc, bson.E{v.Key, v.Value})
		}
	}
	for k, _ := range unvisitedNodes {
		newDoc = append(newDoc, bson.E{k, o.doc[k]})
	}
	return newDoc, nil

}
