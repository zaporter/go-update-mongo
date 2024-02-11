package update

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.viam.com/test"
)

func TestArrayPassthrough(t *testing.T) {
	object := bson.D{{"key", []int{1, 2}}}
	update := bson.D{{"$set", upT{"key": []int{1, 2, 3}}}}
	_, err := UpdateDocument(object, []bson.D{update})
	test.That(t, err, test.ShouldBeNil)
}
