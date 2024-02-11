package update_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	self "github.com/zaporter-work/go-update-mongo/update"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.viam.com/test"
)

type (
	mapT = map[string]any
	objT = bson.D
	upT  = bson.D
)

func ConnectToTestMongo(t *testing.T) *mongo.Client {
	t.Helper()
	ctx := context.Background()
	mongoURI := "mongodb://localhost:27017"
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	test.That(t, err, test.ShouldBeNil)
	test.That(t, mongoClient, test.ShouldNotBeNil)
	err = mongoClient.Ping(ctx, readpref.Primary())
	test.That(t, err, test.ShouldBeNil)
	return mongoClient
}

func TestConnection(t *testing.T) {
	mongo := ConnectToTestMongo(t)
	mongo.Disconnect(context.Background())
}

func TestBehaviorParity(t *testing.T) {
	tests := []struct {
		name             string
		object           objT
		update           upT
		shouldContainErr string
		skip             bool
		allowOutOfOrder  bool
	}{
		{
			name:             "empty to empty",
			object:           bson.D{},
			update:           bson.D{},
			shouldContainErr: "update document must have at least one element",
		},
		// ---------------------- Field Operators -----------------------------
		//
		// $set
		//
		{
			name:   "set field",
			object: objT{{"key", "val1"}},
			update: upT{{
				"$set", mapT{"key": "val2"},
			}},
		},
		{
			name:   "multi set",
			object: objT{{"key", "val1"}},
			update: upT{
				{"$set", mapT{"key": "val2"}},
				// {{"$set", mapT{"key2": "val2"}}},
			},
		},
		{
			name:   "multi set with inc",
			object: objT{{"key", "val1"}, {"a", 1}},
			update: upT{
				{"$set", mapT{"key": "val2"}},
				{"$inc", mapT{"a": 2}},
			},
		},
		{
			name:   "set new field on empty doc",
			object: bson.D{},
			update: upT{{
				"$set", mapT{"key": "newval"},
			}},
		},
		{
			name:   "set an array",
			object: bson.D{},
			update: upT{{
				"$set", mapT{"key": primitive.A{1, 3}},
			}},
		},
		{
			name:   "update an array with set",
			object: bson.D{{"key", primitive.A{1, 2}}},
			update: upT{{
				"$set", mapT{"key": primitive.A{1, 3, 3}},
			}},
		},
		{
			name:   "set swap int to array of strings",
			object: bson.D{{"key", 1}},
			update: upT{{
				"$set", mapT{"key": primitive.A{"hi", "foo", "bar"}},
			}},
		},
		{
			name:   "update var and set new var",
			object: bson.D{{"key", "val1"}},
			update: upT{{
				"$set", mapT{"key": "newval", "newval": "k"},
			}},
		},
		{
			name:   "set nested object",
			object: bson.D{{"key", bson.D{{"subkey", 1}}}},
			update: upT{{
				"$set", mapT{"key.subkey": 2},
			}},
		},
		{
			name:   "set inserts in the correct order (alphabetically)",
			object: bson.D{},
			update: upT{{
				"$set", bson.D{{"b", "newval"}, {"a", "k"}},
			}},
		},
		{
			name:   "set inserts in the correct order (numerically)",
			object: bson.D{},
			update: upT{{
				"$set", bson.D{{"2", "newval"}, {"1", "k"}},
			}},
		},
		//
		// $currentDate
		////
		//{
		//	name:   "currentDate empty",
		//	object: bson.D{},
		//	update: mapT{
		//		"$currentDate": mapT{},
		//	},
		//	skip: true,
		// },
		//{
		//	name:   "currentDate missing field true",
		//	object: bson.D{},
		//	update: mapT{
		//		"$currentDate": mapT{"field": true},
		//	},
		//	skip: true,
		//},
		//{
		//	name:   "currentDate true",
		//	object: bson.D{{"field", 1}},
		//	update: mapT{
		//		"$currentDate": mapT{"field": true},
		//	},
		//	skip: true,
		//},
		//{
		//	name:   "currentDate nested set",
		//	object: bson.D{{"field", 1}},
		//	update: mapT{
		//		"$currentDate": mapT{"unknown.bar": true},
		//	},
		//	skip: true,
		//},
		//{
		//	name:   "currentDate document timestamp",
		//	object: bson.D{{"field", 1}},
		//	update: mapT{
		//		"$currentDate": mapT{"field": mapT{"$type": "timestamp"}},
		//	},
		//	skip: true,
		//},
		//{
		//	name:   "currentDate document date",
		//	object: bson.D{{"field", 1}},
		//	update: mapT{
		//		"$currentDate": mapT{"field": mapT{"$type": "date"}},
		//	},
		//	skip: true,
		//},
		//{
		//	name:   "currentDate document anything else",
		//	object: bson.D{{"field", 1}},
		//	update: mapT{
		//		"$currentDate": mapT{"field": mapT{"$type": "foobar"}},
		//	},
		//	skip: true,
		//},
		//
		// $min
		//
		{
			name:   "min with existing lower value",
			object: objT{{"score", 100}},
			update: upT{{"$min", mapT{"score": 150}}},
		},
		{
			name:   "min with existing higher value",
			object: objT{{"score", 200}},
			update: upT{{"$min", mapT{"score": 150}}},
		},
		{
			name:   "min on non-existent field",
			object: objT{{"score", 100}},
			update: upT{{"$min", mapT{"newScore": 50}}},
		},
		{
			name:   "min with equal value",
			object: objT{{"score", 100}},
			update: upT{{"$min", mapT{"score": 100}}},
		},
		{
			name:   "min on nested field",
			object: objT{{"details", bson.D{{"score", 200}}}},
			update: upT{{"$min", mapT{"details.score": 150}}},
		},
		{
			name:   "min with different types, number less than string",
			object: objT{{"mix", 5}},
			update: upT{{"$min", mapT{"mix": "10"}}},
		},
		{
			name:   "min with different types, string greater than number",
			object: objT{{"mix", "20"}},
			update: upT{{"$min", mapT{"mix": 10}}},
		},
		{
			name:   "min to compare dates",
			object: objT{{"lastUpdate", primitive.NewDateTimeFromTime(time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC))}},
			update: upT{{"$min", mapT{"lastUpdate": primitive.NewDateTimeFromTime(time.Date(2023, 1, 5, 0, 0, 0, 0, time.UTC))}}},
		},
		{
			name:   "min date with existing date later",
			object: objT{{"lastUpdate", primitive.NewDateTimeFromTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))}},
			update: upT{{"$min", mapT{"lastUpdate": primitive.NewDateTimeFromTime(time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC))}}},
		},
		{
			name:   "min on array field should fail",
			object: objT{{"values", primitive.A{10, 20, 30}}},
			update: upT{{"$min", mapT{"values": 5}}},
		},
		{
			name:   "min on empty document",
			object: bson.D{},
			update: upT{{"$min", bson.D{}}},
		},
		{
			name:   "min with mixed number and string fields",
			object: objT{{"num", 100}, {"str", "abc"}},
			update: upT{{"$min", mapT{"num": 50, "str": "xyz"}}},
		},
		{
			name:   "min with embedded document comparison",
			object: objT{{"profile", bson.D{{"age", 30}}}},
			update: upT{{"$min", mapT{"profile.age": 25}}},
		},
		{
			name:   "min updates field in correct order (numerically)",
			object: objT{},
			update: upT{{"$min", bson.D{{"2", 30}, {"1", 20}}}},
		},
		//
		// $max
		//
		{
			name:   "max with existing higher value",
			object: objT{{"score", 200}},
			update: upT{{"$max", mapT{"score": 150}}},
		},
		{
			name:   "max with existing lower value",
			object: objT{{"score", 100}},
			update: upT{{"$max", mapT{"score": 150}}},
		},
		{
			name:   "max on non-existent field",
			object: objT{{"score", 100}},
			update: upT{{"$max", mapT{"newScore": 150}}},
		},
		{
			name:   "max with equal value",
			object: objT{{"score", 150}},
			update: upT{{"$max", mapT{"score": 150}}},
		},
		{
			name:   "max on nested field",
			object: objT{{"details", bson.D{{"score", 100}}}},
			update: upT{{"$max", mapT{"details.score": 150}}},
		},
		{
			name:   "max with different types, number greater than string",
			object: objT{{"mix", 20}},
			update: upT{{"$max", mapT{"mix": "10"}}},
		},
		{
			name:   "max with different types, string less than number",
			object: objT{{"mix", "5"}},
			update: upT{{"$max", mapT{"mix": 10}}},
		},
		{
			name:   "max to compare dates",
			object: objT{{"lastUpdate", primitive.NewDateTimeFromTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC))}},
			update: upT{{"$max", mapT{"lastUpdate": primitive.NewDateTimeFromTime(time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC))}}},
		},
		{
			name:   "max date with existing date earlier",
			object: objT{{"lastUpdate", primitive.NewDateTimeFromTime(time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC))}},
			update: upT{{"$max", mapT{"lastUpdate": primitive.NewDateTimeFromTime(time.Date(2023, 1, 5, 0, 0, 0, 0, time.UTC))}}},
		},
		{
			name:   "max on array field",
			object: objT{{"values", primitive.A{10, 20, 30}}},
			update: upT{{"$max", mapT{"values": 40}}},

		},
		{
			name:   "max on empty document",
			object: bson.D{},
			update: upT{{"$max", bson.D{}}},
		},
		{
			name:   "max with mixed number and string fields",
			object: objT{{"num", 50}, {"str", "abc"}},
			update: upT{{"$max", mapT{"num": 100, "str": "xyz"}}},
		},
		{
			name:   "max with embedded document comparison",
			object: objT{{"profile", bson.D{{"age", 20}}}},
			update: upT{{"$max", mapT{"profile.age": 25}}},
		},
		{
			name:   "max updates field in correct order (numerically)",
			object: objT{},
			update: upT{{"$max", bson.D{{"2", 20}, {"1", 30}}}},
		},
		//
		// $setOnInsert
		// TODO: determine how $setOnInsert should work.
		//
		{
			name:   "setOnInsert on insert",
			object: bson.D{}, // Assuming no document matches the query
			update: upT{
				{"$set", mapT{"item": "apple"}},
				{"$setOnInsert", mapT{"defaultQty": 100}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert does nothing on update",
			object: bson.D{{"item", "apple"}}, // Assuming a document matches the query
			update: upT{
				{"$set", mapT{"item": "banana"}},
				{"$setOnInsert", mapT{"defaultQty": 100}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert with multiple fields on insert",
			object: bson.D{}, // No matching document
			update: upT{
				{"$setOnInsert", mapT{"defaultQty": 50, "status": "available"}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert on nested field on insert",
			object: bson.D{}, // No matching document
			update: upT{
				{"$setOnInsert", mapT{"details.stock": 150, "details.location": "warehouse"}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert with mixed $set and $setOnInsert",
			object: bson.D{}, // No matching document
			update: upT{
				{"$set", mapT{"item": "orange"}},
				{"$setOnInsert", mapT{"item": "kiwi", "defaultQty": 200}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert with existing document, mixed $set and $setOnInsert",
			object: bson.D{{"item", "orange"}}, // Matching document exists
			update: upT{
				{"$set", mapT{"item": "grape"}},
				{"$setOnInsert", mapT{"defaultQty": 200}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert on empty update operation with upsert",
			object: bson.D{}, // No matching document
			update: upT{
				{"$setOnInsert", bson.D{}},
			},
		},
		{
			name:   "setOnInsert updates field in correct order (lexicographically)",
			object: bson.D{}, // Assuming no document matches
			update: upT{
				{"$setOnInsert", bson.D{{"b", "value"}, {"a", "value"}}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert updates field in correct order (numerically)",
			object: bson.D{}, // Assuming no document matches
			update: upT{
				{"$setOnInsert", bson.D{{"2", "value"}, {"1", "value"}}},
			},
			skip: true,
		},
		{
			name:   "setOnInsert with dot notation on array field on insert",
			object: bson.D{}, // No matching document
			update: upT{
				{"$setOnInsert", mapT{"array.0": "firstElement", "array.1": "secondElement"}},
			},
		},
		//
		// $unset
		//
		{
			name:   "unset single field",
			object: objT{{"key", "value"}, {"keep", "this"}},
			update: upT{{"$unset", mapT{"key": ""}}},
		},
		{
			name:   "unset multiple fields",
			object: objT{{"remove1", "value1"}, {"remove2", "value2"}, {"keep", "this"}},
			update: upT{{"$unset", mapT{"remove1": "", "remove2": ""}}},
		},
		{
			name:   "unset non-existent field",
			object: objT{{"key", "value"}},
			update: upT{{"$unset", mapT{"noKey": ""}}},
		},
		{
			name:   "unset nested field",
			object: objT{{"parent", bson.D{{"child", "value"}}}},
			update: upT{{"$unset", mapT{"parent.child": ""}}},
		},
		{
			name:   "unset field in array with dot notation",
			object: objT{{"array", primitive.A{bson.D{{"key", "value"}}}}},
			update: upT{{"$unset", mapT{"array.0.key": ""}}},
		},
		{
			name:   "unset replaces array element with null",
			object: objT{{"array", primitive.A{"value1", "value2"}}},
			update: upT{{"$unset", mapT{"array.1": ""}}},
			/*
			   --- FAIL: TestBehaviorParity/unset_replaces_array_element_with_null (0.00s)
			       lib_test.go:777: Expected: '"[{\"Key\":\"_id\",\"Value\":\"36d30f34-6627-488b-a5d1-4cdb5b79b202\"},{\"Key\":\"array\",\"Value\":[\"value1\",null]}]"'
			           Actual:   '"[{\"Key\":\"_id\",\"Value\":\"36d30f34-6627-488b-a5d1-4cdb5b79b202\"},{\"Key\":\"array\",\"Value\":[\"value1\"]}]"'
			           (Should resemble)!
			           Diff:     '"[{\"Key\":\"_id\",\"Value\":\"36d30f34-6627-488b-a5d1-4cdb5b79b202\"},{\"Key\":\"array\",\"Value\":[\"value1\",null]}]
			*/
			skip: true,
		},
		{
			name:   "unset entire array",
			object: objT{{"array", primitive.A{"value1", "value2"}}},
			update: upT{{"$unset", mapT{"array": ""}}},
		},
		{
			name:   "unset with empty operand",
			object: objT{{"key", "value"}},
			update: upT{{"$unset", bson.D{}}},
		},
		{
			name:   "unset embedded document",
			object: objT{{"doc", bson.D{{"key", "value"}}}},
			update: upT{{"$unset", mapT{"doc": ""}}},
		},
		{
			name:   "unset fields processed in lexicographic order",
			object: objT{{"a", "value1"}, {"b", "value2"}},
			update: upT{{"$unset", bson.D{{"b", ""}, {"a", ""}}}},
		},
		{
			name:   "unset fields processed in numeric order",
			object: objT{{"1", "value1"}, {"2", "value2"}},
			update: upT{{"$unset", bson.D{{"2", ""}, {"1", ""}}}},
		},
		{
			name:   "unset does nothing on non-existent field in empty doc",
			object: bson.D{},
			update: upT{{"$unset", mapT{"noKey": ""}}},
		},
		{
			name:   "unset on nested field in array",
			object: objT{{"array", primitive.A{bson.D{{"nested", "value"}}}}},
			update: upT{{"$unset", mapT{"array.0.nested": ""}}},
		},
		{
			name:   "unset multiple nested fields",
			object: objT{{"level1", bson.D{{"level2", bson.D{{"key1", "value1"}, {"key2", "value2"}}}}}},
			update: upT{{"$unset", mapT{"level1.level2.key1": "", "level1.level2.key2": ""}}},
		},

		//
		// $inc
		//
		{
			name:   "inc unknown field",
			object: objT{{"field", 1}},
			update: upT{{
				"$inc", mapT{"field2": 1},
			}},
		},
		{
			name:   "inc known base field",
			object: objT{{"field", 1}},
			update: upT{{
				"$inc", mapT{"field": 2},
			}},
		},
		{
			name:   "inc zero",
			object: objT{{"field", 1}},
			update: upT{{
				"$inc", mapT{"field": 0},
			}},
		},
		{
			name:   "inc fraction",
			object: objT{{"field", 1}},
			update: upT{{
				"$inc", mapT{"field": 0.5},
			}},
		},
		{
			name:   "inc known base field negative",
			object: objT{{"field", 1}},
			update: upT{{
				"$inc", mapT{"field": -102},
			}},
		},
		{
			name:   "inc nested field",
			object: objT{{"field", bson.M{"nested": 1}}},
			update: upT{{
				"$inc", mapT{"field.nested": -102},
			}},
		},
		{
			name:   "inc negative nested field",
			object: objT{{"field", bson.M{"nested": -1}}},
			update: upT{{
				"$inc", mapT{"field.nested": -102},
			}},
		},
		{
			name:   "inc negative nested non-existent field",
			object: objT{{"field", bson.M{"nested": -1}}},
			update: upT{{
				"$inc", mapT{"field.bar": -102},
			}},
		},
		////
		//// $mul
		////
		{
			name:   "mul unknown field",
			object: objT{{"field", 1}},
			update: upT{{
				"$mul", mapT{"field2": 1},
			}},
		},
		{
			name:   "mul known base field",
			object: objT{{"field", 1}},
			update: upT{{
				"$mul", mapT{"field": 2},
			}},
		},
		{
			name:   "mul zero",
			object: objT{{"field", 1}},
			update: upT{{
				"$mul", mapT{"field": 0},
			}},
		},
		{
			name:   "mul floats",
			object: objT{{"field", 1.5}},
			update: upT{{
				"$mul", mapT{"field": -1.3},
			}},
		},
		{
			name:   "mul small ints",
			object: objT{{"field", 5}},
			update: upT{{
				"$mul", mapT{"field": 5},
			}},
		},
		{
			name:   "mul one large int",
			object: objT{{"field", 3*10 ^ 9}},
			update: upT{{
				"$mul", mapT{"field": 3},
			}},
		},
		{
			name:   "mul two large ints",
			object: objT{{"field", 3*10 ^ 9}},
			update: upT{{
				"$mul", mapT{"field": 4*10 ^ 9},
			}},
		},
		{
			name:   "mul one large int and a float",
			object: objT{{"field", 3*10 ^ 9}},
			update: upT{{
				"$mul", mapT{"field": 1.2},
			}},
		},
		{
			name:   "mul fraction",
			object: objT{{"field", 1}},
			update: upT{{
				"$mul", mapT{"field": 0.5},
			}},
		},
		{
			name:   "mul known base field negative",
			object: objT{{"field", 1}},
			update: upT{{
				"$mul", mapT{"field": -102},
			}},
		},
		{
			name:   "mul nested field",
			object: objT{{"field", bson.M{"nested": 1}}},
			update: upT{{
				"$mul", mapT{"field.nested": -102},
			}},
		},
		{
			name:   "mul negative nested field",
			object: objT{{"field", bson.M{"nested": -1}}},
			update: upT{{
				"$mul", mapT{"field.nested": -102},
			}},
		},
		{
			name:   "mul negative nested non-existent field",
			object: objT{{"field", bson.M{"nested": -1}}},
			update: upT{{
				"$mul", mapT{"field.bar": -102},
			}},
		},
		////
		//// $rename
		////
		{
			name:   "rename simple",
			object: objT{{"field", 1}},
			update: upT{{
				"$rename", mapT{"field": "alias"},
			}},
		},
		{
			name:   "rename simple missing",
			object: objT{{"field", 1}},
			update: upT{{
				"$rename", mapT{"fieldnope": "alias"},
			}},
		},
		{
			name:   "set and rename at the same time",
			object: objT{},
			update: upT{
				{"$set", mapT{"field": "value"}},
				{"$rename", mapT{"field": "alias"}},
			},
			// mongo prevents this but ferret allows this
			skip: true,
		},
		{
			name:   "rename multiple",
			object: objT{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{{
				"$rename", mapT{"field": "a", "twin": "b", "map": "c"},
			}},
			allowOutOfOrder: true,
		},
		{
			name:   "rename nested down to first level",
			object: objT{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{{
				"$rename", mapT{"field.a": "a"},
			}},
			shouldContainErr: "failed to update document",
		},
		{
			name:   "rename nested within same level",
			object: objT{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{{
				"$rename", mapT{"field.a": "field.chicken"},
			}},
			shouldContainErr: "failed to update document",
		},
		//// ---------------------- Bitwise Operators -----------------------------
		////
		//// $bit
		////
		{
			name:   "bit and",
			object: objT{{"field", 13}},
			update: upT{{
				"$bit", mapT{"field": mapT{"and": 10}},
			}},
		},
		{
			name:   "bit or",
			object: objT{{"field", 13}},
			update: upT{{
				"$bit", mapT{"field": mapT{"or": 10}},
			}},
		},
		{
			name:   "bit xor",
			object: objT{{"field", 13}},
			update: upT{{
				"$bit", mapT{"field": mapT{"xor": 10}},
			}},
		},
		// ---------------------- Array Operators -----------------------------
		//
		// $addToSet
		//

		{
			name:   "addToSet on existing array with new element",
			object: objT{{"items", primitive.A{"item1", "item2"}}},
			update: upT{{"$addToSet", mapT{"items": "item3"}}},
		},
		{
			name:   "addToSet on existing array with existing element",
			object: objT{{"items", primitive.A{"item1", "item2"}}},
			update: upT{{"$addToSet", mapT{"items": "item2"}}},
		},
		{
			name:   "addToSet creates new array field if not exist",
			object: bson.D{},
			update: upT{{"$addToSet", mapT{"newItems": "item1"}}},
		},
		{
			name:             "addToSet fails on non-array field",
			object:           objT{{"item", "singleValue"}},
			update:           upT{{"$addToSet", mapT{"item": "newValue"}}},
			shouldContainErr: "led to update document",
		},
		{
			name:   "addToSet appends entire array as single element",
			object: objT{{"arrays", primitive.A{primitive.A{"a", "b"}}}},
			update: upT{{"$addToSet", mapT{"arrays": primitive.A{"c", "d"}}}},
			// mongo fails because (nested arrays are not supported), however ferretDB doesn't fail
			skip:             true,
			shouldContainErr: "test",
		},
		{
			name:   "addToSet with $each to add multiple unique elements",
			object: objT{{"items", primitive.A{"item1", "item2"}}},
			update: upT{{"$addToSet", mapT{"items": mapT{"$each": primitive.A{"item3", "item4"}}}}},
		},
		{
			name:   "addToSet with $each where some elements already exist",
			object: objT{{"items", primitive.A{"item1", "item2"}}},
			update: upT{{"$addToSet", mapT{"items": mapT{"$each": primitive.A{"item2", "item3"}}}}},
		},
		{
			name:   "addToSet adds document to array",
			object: objT{{"docs", primitive.A{bson.D{{"key", "value1"}}}}},
			update: upT{{"$addToSet", mapT{"docs": bson.D{{"key", "value2"}}}}},
		},
		{
			name:   "addToSet does not add duplicate document to array",
			object: objT{{"docs", primitive.A{bson.D{{"key", "value1"}}}}},
			update: upT{{"$addToSet", mapT{"docs": bson.D{{"key", "value1"}}}}},
		},
		{
			name:   "addToSet on embedded document array",
			object: objT{{"parent", bson.D{{"children", primitive.A{"child1"}}}}},
			update: upT{{"$addToSet", mapT{"parent.children": "child2"}}},
		},
		{
			name:   "addToSet with empty operand",
			object: objT{{"items", primitive.A{"item1"}}},
			update: upT{{"$addToSet", bson.D{}}},
		},
		//
		// $pop
		//
		{
			name:   "pop last element",
			object: objT{{"array", primitive.A{1, 2, 3}}},
			update: upT{{"$pop", mapT{"array": 1}}},
		},
		{
			name:   "pop first element",
			object: objT{{"array", primitive.A{1, 2, 3}}},
			update: upT{{"$pop", mapT{"array": -1}}},
		},
		{
			name:   "pop last element leaves empty array",
			object: objT{{"array", primitive.A{1}}},
			update: upT{{"$pop", mapT{"array": 1}}},
		},
		{
			name:   "pop first element leaves empty array",
			object: objT{{"array", primitive.A{1}}},
			update: upT{{"$pop", mapT{"array": -1}}},
		},
		{
			name:   "pop on empty array does nothing",
			object: objT{{"array", primitive.A{}}},
			update: upT{{"$pop", mapT{"array": 1}}},
		},
		{
			name:             "pop on non-array field fails",
			object:           objT{{"nonArray", "notAnArray"}},
			update:           upT{{"$pop", mapT{"nonArray": 1}}},
			shouldContainErr: "contains an element of non-array type",
		},
		{
			name:   "pop nested array last element",
			object: objT{{"nested", bson.D{{"array", primitive.A{1, 2, 3}}}}},
			update: upT{{"$pop", mapT{"nested.array": 1}}},
		},
		{
			name:   "pop nested array first element",
			object: objT{{"nested", bson.D{{"array", primitive.A{1, 2, 3}}}}},
			update: upT{{"$pop", mapT{"nested.array": -1}}},
		},
		{
			name:   "pop with empty operand",
			object: objT{{"array", primitive.A{1, 2, 3}}},
			update: upT{{"$pop", bson.D{}}},
		},
		{
			name:   "pop multiple arrays",
			object: objT{{"array1", primitive.A{1, 2, 3}}, {"array2", primitive.A{"a", "b", "c"}}},
			update: upT{{"$pop", mapT{"array1": 1, "array2": -1}}},
		},
		{
			name:   "pop from array of documents",
			object: objT{{"docs", primitive.A{bson.D{{"key", "value1"}}, bson.D{{"key", "value2"}}}}},
			update: upT{{"$pop", mapT{"docs": 1}}},
		},
		{
			name:   "pop from array with mixed types",
			object: objT{{"mixed", primitive.A{1, "two", bson.D{{"key", "value"}}}}},
			update: upT{{"$pop", mapT{"mixed": -1}}},
		},
		{
			name:   "pop does nothing on non-existent field",
			object: objT{{"someField", primitive.A{1, 2, 3}}},
			update: upT{{"$pop", mapT{"nonExistentField": 1}}},
		},
		{
			name:   "pop last element with numeric field name",
			object: objT{{"1", primitive.A{1, 2, 3}}},
			update: upT{{"$pop", mapT{"1": 1}}},
		},
		//
		// $pull
		//
		{
			name:   "pull single value",
			object: objT{{"fruits", primitive.A{"apple", "banana", "orange"}}},
			update: upT{{"$pull", mapT{"fruits": "banana"}}},
		},
		{
			name:   "pull multiple values with $in",
			object: objT{{"fruits", primitive.A{"apple", "banana", "orange", "grape"}}},
			update: upT{{"$pull", mapT{"fruits": mapT{"$in": primitive.A{"banana", "grape"}}}}},
			// pull with conditions just doesn't work
			skip: true,
		},
		{
			name:   "pull with condition",
			object: objT{{"scores", primitive.A{85, 92, 75, 91}}},
			update: upT{{"$pull", mapT{"scores": mapT{"$lt": 90}}}},
			// pull with conditions just doesn't work
			skip: true,
		},
		{
			name:   "pull embedded document by exact match",
			object: objT{{"comments", primitive.A{bson.D{{"author", "joe"}, {"score", 3}}, bson.D{{"author", "jane"}, {"score", 4}}}}},
			update: upT{{"$pull", mapT{"comments": bson.D{{"author", "joe"}, {"score", 3}}}}},
		},
		{
			name:   "pull from nested array with dot notation",
			object: objT{{"blogPosts", bson.D{{"comments", primitive.A{"good post", "needs work", "excellent"}}}}},
			update: upT{{"$pull", mapT{"blogPosts.comments": "needs work"}}},
		},
		{
			name:   "pull entire document from array",
			object: objT{{"users", primitive.A{bson.D{{"name", "John"}, {"age", 30}}, bson.D{{"name", "Jane"}, {"age", 25}}}}},
			update: upT{{"$pull", mapT{"users": bson.D{{"name", "John"}, {"age", 30}}}}},
		},
		{
			name:   "pull with empty operand",
			object: objT{{"items", primitive.A{"apple", "banana", "cucumber"}}},
			update: upT{{"$pull", bson.D{}}},
		},
		{
			name:   "pull does not match order in array",
			object: objT{{"fruits", primitive.A{"apple", "banana"}}},
			update: upT{{"$pull", mapT{"fruits": primitive.A{"banana", "apple"}}}},
		},
		{
			name:   "pull by condition on nested field",
			object: objT{{"orders", primitive.A{bson.D{{"item", "xyz"}, {"qty", 25}}, bson.D{{"item", "abc"}, {"qty", 50}}}}},
			update: upT{{"$pull", mapT{"orders": bson.D{{"qty", mapT{"$lt": 30}}}}}},
			// pull with conditions just doesn't work
			skip: true,
		},
		{
			name:   "pull non-existent value does nothing",
			object: objT{{"tags", primitive.A{"code", "design", "write"}}},
			update: upT{{"$pull", mapT{"tags": "publish"}}},
		},
		{
			name:   "pull with regex",
			object: objT{{"words", primitive.A{"hello", "world", "example", "sample"}}},
			update: upT{{"$pull", mapT{"words": mapT{"$regex": "^ex"}}}},
			// pull with conditions just doesn't work
			skip: true,
		},
		{
			name:   "pull from multiple fields",
			object: objT{{"fruits", primitive.A{"apple", "banana"}}, {"veggies", primitive.A{"carrot", "celery"}}},
			update: upT{{"$pull", mapT{"fruits": "apple", "veggies": "carrot"}}},
		},
		{
			name:   "pull with gte condition",
			object: objT{{"products", primitive.A{bson.D{{"name", "laptop"}, {"price", 1000}}, bson.D{{"name", "phone"}, {"price", 500}}}}},
			update: upT{{"$pull", mapT{"products": bson.D{{"price", mapT{"$gte": 750}}}}}},
			// pull with conditions just doesn't work
			skip: true,
		},
		//
		// $push
		//

		{
			name:   "push single value to existing array",
			object: objT{{"scores", primitive.A{44, 78, 38, 80}}},
			update: upT{{"$push", mapT{"scores": 89}}},
		},
		{
			name:   "push single value to non-existent array",
			object: objT{{"name", "joe"}},
			update: upT{{"$push", mapT{"scores": 90}}},
		},
		{
			name:             "push fails on non-array field",
			object:           objT{{"score", 100}},
			update:           upT{{"$push", mapT{"score": 100}}},
			shouldContainErr: "must be an array",
		},
		{
			name:             "push array as single element",
			object:           objT{{"items", primitive.A{"item1"}}},
			update:           upT{{"$push", mapT{"items": primitive.A{"item2", "item3"}}}},
			skip:             true, // mongo denies this but ferret accepts this
			shouldContainErr: "nested arrays are not supported",
		},
		{
			name:   "push with $each modifier",
			object: objT{{"scores", primitive.A{90}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{92, 85}}}}},
		},
		{
			name:   "push with $each and $sort modifiers",
			object: objT{{"quizzes", primitive.A{bson.D{{"wk", 1}, {"score", 10}}}}},
			update: upT{{"$push", mapT{"quizzes": mapT{"$each": primitive.A{bson.D{{"wk", 2}, {"score", 8}}}, "$sort": bson.D{{"score", -1}}}}}},
		},
		{
			name:   "push with $each, $sort, and $slice modifiers",
			object: objT{{"quizzes", primitive.A{bson.D{{"wk", 1}, {"score", 10}}, bson.D{{"wk", 2}, {"score", 8}}}}},
			update: upT{{"$push", mapT{"quizzes": mapT{"$each": primitive.A{bson.D{{"wk", 3}, {"score", 7}}}, "$sort": bson.D{{"score", -1}}, "$slice": 2}}}},
			// ferret forgets to push the value.
			skip: true,
		},
		{
			name:   "push with $each and $position modifiers",
			object: objT{{"scores", primitive.A{80, 85}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{75}, "$position": 0}}}},
			skip:   true,
			// ferret pushes to the wrong location
		},
		{
			name:   "push on nested field",
			object: objT{{"student", bson.D{{"scores", primitive.A{44, 78}}}}},
			update: upT{{"$push", mapT{"student.scores": 88}}},
		},
		{
			name:   "push with empty operand",
			object: objT{{"scores", primitive.A{44, 78, 38, 80}}},
			update: upT{{"$push", bson.D{}}},
		},
		{
			name:   "push with $each and complex $sort modifier",
			object: objT{{"students", primitive.A{bson.D{{"name", "Alice"}, {"grade", 90}}, bson.D{{"name", "Bob"}, {"grade", 85}}}}},
			update: upT{{"$push", mapT{"students": mapT{"$each": primitive.A{bson.D{{"name", "Charlie"}, {"grade", 95}}}, "$sort": bson.D{{"grade", -1}}}}}},
			skip:   true,
			// ferret pushed it but didn't put it in the wright location
		},
		{
			name:   "push with $each, $sort (multiple fields), and $slice modifiers",
			object: objT{{"students", primitive.A{bson.D{{"name", "Alice"}, {"age", 20}, {"grade", 90}}, bson.D{{"name", "Bob"}, {"age", 22}, {"grade", 85}}}}},
			update: upT{{"$push", mapT{"students": mapT{"$each": primitive.A{bson.D{{"name", "Charlie"}, {"age", 21}, {"grade", 95}}}, "$sort": bson.D{{"grade", -1}, {"age", 1}}, "$slice": 2}}}},
			skip:   true,
		},
		//
		// $pushAll
		//

		{
			name:   "pullAll simple values from array",
			object: objT{{"items", primitive.A{"apple", "banana", "orange", "apple"}}},
			update: upT{{"$pullAll", mapT{"items": primitive.A{"apple", "orange"}}}},
		},
		{
			name:   "pullAll with no matching values",
			object: objT{{"items", primitive.A{"apple", "banana", "orange"}}},
			update: upT{{"$pullAll", mapT{"items": primitive.A{"grape", "pear"}}}},
		},
		{
			name:   "pullAll on non-existent field",
			object: objT{{"items", primitive.A{"apple", "banana", "orange"}}},
			update: upT{{"$pullAll", mapT{"nonExistent": primitive.A{"apple"}}}},
			skip:   true,
			// ferret inserts it but mongo doesn't
		},
		{
			name:   "pullAll exact objects from array",
			object: objT{{"objects", primitive.A{bson.D{{"key", "value"}}, bson.D{{"key", "value2"}}, bson.D{{"key", "value"}}}}},
			update: upT{{"$pullAll", mapT{"objects": primitive.A{bson.D{{"key", "value"}}}}}},
		},
		{
			name:   "pullAll exact array from nested array",
			object: objT{{"arrays", primitive.A{primitive.A{"one", "two"}, primitive.A{"three", "four"}, primitive.A{"one", "two"}}}},
			update: upT{{"$pullAll", mapT{"arrays": primitive.A{primitive.A{"one", "two"}}}}},
			skip:   true,
			// mongo doesn't support nested arrays
		},
		{
			name:   "pullAll from nested field",
			object: objT{{"parent", bson.D{{"child", primitive.A{"a", "b", "c", "b"}}}}},
			update: upT{{"$pullAll", mapT{"parent.child": primitive.A{"b"}}}},
		},
		{
			name:   "pullAll mixed types",
			object: objT{{"mixed", primitive.A{"a", 1, "b", 2, "a"}}},
			update: upT{{"$pullAll", mapT{"mixed": primitive.A{"a", 2}}}},
		},
		{
			name:   "pullAll with empty operand",
			object: objT{{"items", primitive.A{"apple", "banana", "orange"}}},
			update: upT{{"$pullAll", bson.D{}}},
		},
		{
			name:   "pullAll multiple fields",
			object: objT{{"fruits", primitive.A{"apple", "banana", "orange"}}, {"veggies", primitive.A{"carrot", "pea"}}},
			update: upT{{"$pullAll", mapT{"fruits": primitive.A{"apple"}, "veggies": primitive.A{"pea"}}}},
		},
		{
			name:   "pullAll does not match sub-documents",
			object: objT{{"docs", primitive.A{bson.D{{"key", "value"}}, bson.D{{"key", "different"}}}}},
			update: upT{{"$pullAll", mapT{"docs": primitive.A{bson.D{{"key", "value"}}}}}},
		},
		{
			name:   "pullAll exact match required for documents",
			object: objT{{"docs", primitive.A{bson.D{{"key1", "value1"}, {"key2", "value2"}}, bson.D{{"key1", "value1"}}}}},
			update: upT{{"$pullAll", mapT{"docs": primitive.A{bson.D{{"key1", "value1"}, {"key2", "value2"}}}}}},
		},
		{
			name:   "pullAll removes all instances",
			object: objT{{"nums", primitive.A{1, 2, 3, 1, 2, 3, 1}}},
			update: upT{{"$pullAll", mapT{"nums": primitive.A{1, 3}}}},
		},
		{
			name:   "pullAll from array of arrays",
			object: objT{{"arrays", primitive.A{primitive.A{1, 2}, primitive.A{3, 4}, primitive.A{1, 2}}}},
			update: upT{{"$pullAll", mapT{"arrays": primitive.A{primitive.A{1, 2}}}}},
			skip:   true,
			// mongo doesn't support nested arrays
		},
		// --------------------- modifiers -----------------------
		//
		// $position
		//
		{
			name:   "push with position at start",
			object: objT{{"scores", primitive.A{100}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{50, 60, 70}, "$position": 0}}}},
			skip:   true,
		},
		{
			name:   "push with position in middle",
			object: objT{{"scores", primitive.A{50, 60, 70, 100}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{20, 30}, "$position": 2}}}},
			skip:   true,
		},
		{
			name:   "push with position at end",
			object: objT{{"scores", primitive.A{50, 60}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{70, 80}, "$position": 2}}}}, // Assuming array length is 2, positions beyond length should push at end
		},
		{
			name:   "push with negative position",
			object: objT{{"scores", primitive.A{50, 60, 70, 100}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{90}, "$position": -1}}}},
			skip:   true,
		},
		{
			name:   "push with negative position multiple elements",
			object: objT{{"scores", primitive.A{50, 60, 70, 100}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{80, 90}, "$position": -2}}}},
			skip:   true,
		},
		{
			name:   "push with position beyond array length",
			object: objT{{"scores", primitive.A{50, 60, 70}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{100, 110}, "$position": 10}}}}, // Should add at the end
		},
		{
			name:   "push with zero position on empty array",
			object: objT{{"scores", primitive.A{}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{10, 20, 30}, "$position": 0}}}},
		},
		{
			name:   "push with negative position beyond array length",
			object: objT{{"scores", primitive.A{50, 60}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{10, 20}, "$position": -5}}}}, // Should add at the beginning
			skip:   true,
		},
		{
			name:   "push with negative position to empty array",
			object: objT{{"scores", primitive.A{}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{10, 20}, "$position": -1}}}}, // Should add at the beginning as the array is empty
		},
		{
			name:   "push with position at exact array length",
			object: objT{{"scores", primitive.A{50, 60, 70}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{80}, "$position": 3}}}}, // Should add at the end, equivalent to not specifying $position
		},
		//
		// $slice
		//
		{
			name:   "slice from the end",
			object: objT{{"scores", primitive.A{40, 50, 60}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{80, 78, 86}, "$slice": -5}}}},
			skip:   true,
		},
		{
			name:   "slice from the front",
			object: objT{{"scores", primitive.A{89, 90}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{100, 20}, "$slice": 3}}}},
			skip:   true,
		},
		{
			name:   "update array using slice only",
			object: objT{{"scores", primitive.A{89, 70, 100, 20}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{}, "$slice": -3}}}},
			skip:   true,
		},
		{
			name:   "slice to empty array",
			object: objT{{"scores", primitive.A{89, 90, 100}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{}, "$slice": 0}}}},
			skip:   true,
		},
		{
			name:   "slice with negative number larger than array length",
			object: objT{{"scores", primitive.A{1, 2}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{3, 4, 5}, "$slice": -10}}}},
		},
		{
			name:   "slice with positive number larger than array length",
			object: objT{{"scores", primitive.A{1, 2, 3}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{4, 5}, "$slice": 10}}}},
		},
		{
			name:   "slice without each modifier should error",
			object: objT{{"scores", primitive.A{1, 2, 3}}},
			update: upT{{"$push", mapT{"scores": mapT{"$slice": -2}}}}, // This is an invalid operation and should result in an error
			skip:   true,
		},
		{
			name:   "slice with each and slice at the end",
			object: objT{{"scores", primitive.A{10, 20, 30}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{40, 50}, "$slice": -3}}}},
			skip:   true,
		},
		{
			name:   "slice with each and slice at the front",
			object: objT{{"scores", primitive.A{10, 20, 30}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{40, 50}, "$slice": 3}}}},
			skip:   true,
		},
		{
			name:   "slice keeps array size consistent",
			object: objT{{"scores", primitive.A{1, 2, 3, 4, 5}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{}, "$slice": -3}}}},
			skip:   true,
		},
		{
			name:   "slice with positive slice on small array",
			object: objT{{"scores", primitive.A{1, 2}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{3}, "$slice": 2}}}},
			skip:   true,
		},
		{
			name:   "slice with zero to empty the array",
			object: objT{{"scores", primitive.A{1, 2, 3}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{4, 5, 6}, "$slice": 0}}}},
			skip:   true,
		},
		{
			name:   "slice with negative number to keep last elements",
			object: objT{{"scores", primitive.A{1, 2, 3, 4, 5}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{6, 7, 8}, "$slice": -3}}}},
			skip:   true,
		},
		{
			name:   "slice to maintain a maximum array length",
			object: objT{{"scores", primitive.A{1, 2, 3, 4}}},
			update: upT{{"$push", mapT{"scores": mapT{"$each": primitive.A{5, 6, 7, 8, 9}, "$slice": 5}}}},
			skip:   true,
		},

		//
		// $sort
		//

		{
			name:   "sort array of documents by field ascending",
			object: objT{{"quizzes", primitive.A{bson.D{{"id", 1}, {"score", 6}}, bson.D{{"id", 2}, {"score", 9}}}}},
			update: upT{{"$push", mapT{"quizzes": mapT{"$each": primitive.A{bson.D{{"id", 3}, {"score", 8}}, bson.D{{"id", 4}, {"score", 7}}, bson.D{{"id", 5}, {"score", 6}}}, "$sort": mapT{"score": 1}}}}},
			skip:   true,
		},
		{
			name:   "sort array of documents by field descending",
			object: objT{{"quizzes", primitive.A{bson.D{{"id", 1}, {"score", 6}}, bson.D{{"id", 2}, {"score", 9}}}}},
			update: upT{{"$push", mapT{"quizzes": mapT{"$each": primitive.A{bson.D{{"id", 3}, {"score", 8}}, bson.D{{"id", 4}, {"score", 7}}, bson.D{{"id", 5}, {"score", 6}}}, "$sort": mapT{"score": -1}}}}},
			skip:   true,
		},
		{
			name:   "sort array of integers ascending",
			object: objT{{"tests", primitive.A{89, 70, 89, 50}}},
			update: upT{{"$push", mapT{"tests": mapT{"$each": primitive.A{40, 60}, "$sort": 1}}}},
			skip:   true,
		},
		{
			name:   "sort array of integers descending",
			object: objT{{"tests", primitive.A{89, 70, 89, 50}}},
			update: upT{{"$push", mapT{"tests": mapT{"$each": primitive.A{40, 60}, "$sort": -1}}}},
			skip:   true,
		},
		{
			name:   "update array using sort only descending",
			object: objT{{"tests", primitive.A{89, 70, 100, 20}}},
			update: upT{{"$push", mapT{"tests": mapT{"$each": primitive.A{}, "$sort": -1}}}},
			skip:   true,
		},
		{
			name:   "update array using sort only ascending",
			object: objT{{"tests", primitive.A{89, 70, 100, 20}}},
			update: upT{{"$push", mapT{"tests": mapT{"$each": primitive.A{}, "$sort": 1}}}},
			skip:   true,
		},
		{
			name:   "sort array of mixed types",
			object: objT{{"mix", primitive.A{"string", 5, "10", 2}}},
			update: upT{{"$push", mapT{"mix": mapT{"$each": primitive.A{1, "a"}, "$sort": 1}}}},
			skip:   true,
		},
		{
			name:   "sort with empty array",
			object: objT{{"emptyTest", primitive.A{}}},
			update: upT{{"$push", mapT{"emptyTest": mapT{"$each": primitive.A{}, "$sort": 1}}}},
		},
		{
			name:   "sort embedded document array by nested field ascending",
			object: objT{{"items", primitive.A{bson.D{{"id", 2}, {"detail", bson.D{{"score", 9}}}}, bson.D{{"id", 1}, {"detail", bson.D{{"score", 6}}}}}}},
			update: upT{{"$push", mapT{"items": mapT{"$each": primitive.A{bson.D{{"id", 3}, {"detail", bson.D{{"score", 8}}}}}, "$sort": mapT{"detail.score": 1}}}}},
			skip:   true,
		},
		{
			name:   "sort embedded document array by nested field descending",
			object: objT{{"items", primitive.A{bson.D{{"id", 2}, {"detail", bson.D{{"score", 9}}}}, bson.D{{"id", 1}, {"detail", bson.D{{"score", 6}}}}}}},
			update: upT{{"$push", mapT{"items": mapT{"$each": primitive.A{bson.D{{"id", 3}, {"detail", bson.D{{"score", 8}}}}}, "$sort": mapT{"detail.score": -1}}}}},
			skip:   true,
		},

		//
		// $[] is not supported by ferretDB
		//
		{
			name:   "increment all array elements",
			object: objT{{"grades", bson.A{85, 82, 80}}},
			update: upT{{"$inc", mapT{"grades.$[]": 10}}},
			skip:   true,
		},
		{
			name:   "set all array elements",
			object: objT{{"status", bson.A{"pending", "pending"}}},
			update: upT{{"$set", mapT{"status.$[]": "complete"}}},
			skip:   true,
		},
		{
			name: "modify all embedded document fields in array",
			object: objT{
				{"grades", bson.A{
					bson.D{{"grade", 80}, {"mean", 75}, {"std", 8}},
					bson.D{{"grade", 85}, {"mean", 90}, {"std", 6}},
				}},
			},
			update: upT{{"$inc", mapT{"grades.$[].std": -2}}},
			skip:   true,
		},
		{
			name: "modify specific field in all embedded documents in array",
			object: objT{{
				"items", bson.A{
					bson.D{{"name", "item1"}, {"quantity", 10}},
					bson.D{{"name", "item2"}, {"quantity", 5}},
				},
			}},
			update: upT{{"$set", mapT{"items.$[].quantity": 0}}},
			skip:   true,
		},
		{
			name: "increment all elements in nested arrays",
			object: objT{
				{
					"nested", primitive.A{
						primitive.A{primitive.A{1, 2}, primitive.A{3, 4}},
						primitive.A{primitive.A{5, 6}, primitive.A{7, 8}},
					},
				},
			},
			update: upT{{"$inc", mapT{"nested.$[].$[]": 1}}},
			skip:   true,
		},
		{
			name:   "no-op with $[] and non-existent field",
			object: objT{{"grades", primitive.A{85, 82, 80}}},
			update: upT{{"$inc", mapT{"nonExistent.$[]": 10}}},
			skip:   true,
		},
		{
			name:   "update all elements with negation query",
			object: objT{{"grades", primitive.A{85, 82, 80, 100}}},
			update: upT{{"$inc", mapT{"grades.$[]": 10}}},
			skip:   true,
		},
		{
			name: "update all elements in array of arrays",
			object: objT{{
				"matrix", primitive.A{
					primitive.A{1, 2, 3},
					primitive.A{4, 5, 6},
				},
			}},
			update: upT{{"$inc", mapT{"matrix.$[].$[]": 1}}},
			skip:   true,
		},
		{
			name: "update all elements in deeply nested arrays",
			object: objT{{
				"deepNested", primitive.A{
					bson.D{{"level2", primitive.A{
						bson.D{{"level3", primitive.A{1, 2}}},
					}}},
				},
			}},
			update: upT{{"$inc", mapT{"deepNested.$[].level2.$[].level3.$[]": 1}}},
			skip:   true,
		},
		{
			name:   "set all elements to specific value in mixed-type array",
			object: objT{{"mixed", primitive.A{"string", 42, true}}},
			update: upT{{"$set", mapT{"mixed.$[]": "updated"}}},
			skip:   true,
		},
		{
			name:   "update with empty array does nothing",
			object: objT{{"emptyArray", primitive.A{}}},
			update: upT{{"$set", mapT{"emptyArray.$[]": "no-op"}}},
			skip:   true,
		},
		//
		// $[<identifier>]
		// ArrayFilters are not yet implemented in FerretDB
		// Untested for now
		//
		// {
		// 	name:         "update matching array elements",
		// 	object:       objT{{"grades", primitive.A{95, 92, 90, 150}}},
		// 	update:       upT{{"$set", mapT{"grades.$[elem]": 100}}},
		// 	arrayFilters: []mapT{{"elem": mapT{"$gte": 100}}},
		// },
		// {
		// 	name:         "update nested fields in matching array elements",
		// 	object:       objT{{"students", primitive.A{bson.D{{"grade", 85}, {"mean", 75}}}}},
		// 	update:       upT{{"$set", mapT{"students.$[elem].mean": 100}}},
		// 	arrayFilters: []mapT{{"elem.grade": mapT{"$gte": 85}}},
		// },
		// {
		// 	name:         "no matching element with arrayFilters",
		// 	object:       objT{{"grades", primitive.A{80, 82, 85}}},
		// 	update:       upT{{"$set", mapT{"grades.$[elem]": 100}}},
		// 	arrayFilters: []mapT{{"elem": mapT{"$gte": 90}}},
		// },
		// {
		// 	name:         "update with multiple conditions in arrayFilters",
		// 	object:       objT{{"students", primitive.A{bson.D{{"grade", 90}, {"std", 6}}, bson.D{{"grade", 85}, {"std", 4}}}}},
		// 	update:       upT{{"$inc", mapT{"students.$[elem].std": -1}}},
		// 	arrayFilters: []mapT{{"elem.grade": mapT{"$gte": 85}, "elem.std": mapT{"$gte": 5}}},
		// },
		// {
		// 	name:         "update array elements using negation in arrayFilters",
		// 	object:       objT{{"alumni", primitive.A{bson.D{{"level", "Master"}}, bson.D{{"level", "Bachelor"}}}}},
		// 	update:       upT{{"$set", mapT{"alumni.$[degree].gradcampaign": 1}}},
		// 	arrayFilters: []mapT{{"degree.level": mapT{"$ne": "Bachelor"}}},
		// },
		// {
		// 	name:         "update nested arrays",
		// 	object:       objT{{"departments", primitive.A{bson.D{{"team", primitive.A{bson.D{{"name", "Engineering"}, {"members", 10}}}}}}}},
		// 	update:       upT{{"$set", mapT{"departments.$[dept].team.$[team].members": 12}}},
		// 	arrayFilters: []mapT{{"dept.team.name": "Engineering"}, {"team.name": "Engineering"}},
		// },
		// {
		// 	name:         "update matching elements in multiple arrays",
		// 	object:       objT{{"multiGrades", primitive.A{primitive.A{95, 100}, primitive.A{92, 100}}}},
		// 	update:       upT{{"$set", mapT{"multiGrades.$[arr].$[elem]": 100}}},
		// 	arrayFilters: []mapT{{"arr": mapT{"$gte": 0}}, {"elem": mapT{"$gte": 95}}},
		// },
		// {
		// 	name:         "update array elements with specific object structure",
		// 	object:       objT{{"products", primitive.A{bson.D{{"name", "apple"}, {"price", 1}}, bson.D{{"name", "banana"}, {"price", 2}}}}},
		// 	update:       upT{{"$set", mapT{"products.$[item].price": 0.5}}},
		// 	arrayFilters: []mapT{{"item.name": "banana"}},
		// },
		// {
		// 	name:         "update without matching arrayFilters condition",
		// 	object:       objT{{"scores", primitive.A{100, 200, 300}}},
		// 	update:       upT{{"$set", mapT{"scores.$[score]": 250}}},
		// 	arrayFilters: []mapT{{"score": mapT{"$lt": 100}}},
		// },
		// {
		// 	name:         "complex condition in arrayFilters",
		// 	object:       objT{{"people", primitive.A{bson.D{{"age", 30}, {"name", "John"}}, bson.D{{"age", 25}, {"name", "Jane"}}}}},
		// 	update:       upT{{"$set", mapT{"people.$[person].active": true}}},
		// 	arrayFilters: []mapT{{"person.age": mapT{"$gte": 30}}},
		// },
		// {
		// 	name:         "updating based on multiple arrayFilters conditions",
		// 	object:       objT{{"classes", primitive.A{bson.D{{"students", primitive.A{bson.D{{"id", 1}, {"score", 80}}, bson.D{{"id", 2}, {"score", 90}}}}}}}},
		// 	update:       upT{{"$set", mapT{"classes.$[class].students.$[student].passed": true}}},
		// 	arrayFilters: []mapT{{"class.students.id": mapT{"$gte": 1}}, {"student.score": mapT{"$gte": 75}}},
		// },
	}
	ctx := context.Background()
	client := ConnectToTestMongo(t)
	defer client.Disconnect(context.Background())
	db := client.Database("behaviorDB")
	col := db.Collection("parity")
	err := col.Drop(ctx)
	test.That(t, err, test.ShouldBeNil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip()
			}
			tcObjectWithID := bson.D{{Key: "_id", Value: uuid.New().String()}}
			tcObjectWithID = append(tcObjectWithID, tc.object...)
			// perform operation in mongo
			mongoResult, mongoErr := performMongoUpdate(t, ctx, col, tcObjectWithID, tc.update)
			// perform my in-memory operation
			myResult, myError := self.UpdateDocument(tcObjectWithID, tc.update)

			if tc.shouldContainErr == "" {
				test.That(t, mongoErr, test.ShouldBeNil)
				test.That(t, myError, test.ShouldBeNil)
				if tc.allowOutOfOrder {
					// if out of order, sort top level keys. Not perfect but good enough
					slices.SortStableFunc(mongoResult, func(a, b primitive.E) int { return cmp.Compare(a.Key, b.Key) })
					slices.SortStableFunc(myResult, func(a, b primitive.E) int { return cmp.Compare(a.Key, b.Key) })
				}

				// converting to json leads to more human readable test failure messages
				mongoJSON, err := json.Marshal(mongoResult)
				test.That(t, err, test.ShouldBeNil)
				myJSON, err := json.Marshal(myResult)
				test.That(t, err, test.ShouldBeNil)
				test.That(t, string(myJSON), test.ShouldContainSubstring, "_id")
				test.That(t, string(myJSON), test.ShouldResemble, string(mongoJSON))
			} else {
				// ensure the mongo error is present (don't care what it is)
				test.That(t, mongoErr, test.ShouldNotBeNil)
				// ensure we contain the desired error
				test.That(t, fmt.Sprint(myError), test.ShouldContainSubstring, tc.shouldContainErr)
			}
		})
	}
}

func performMongoUpdate(t *testing.T,
	ctx context.Context,
	col *mongo.Collection,
	object bson.D,
	update bson.D,
) (result bson.D, err error) {
	t.Helper()
	insertRes, err := col.InsertOne(ctx, object)
	test.That(t, err, test.ShouldBeNil)
	// use mongodb generated _id
	id := insertRes.InsertedID
	_, err = col.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return
	}
	// test.That(t, updateResult.ModifiedCount, test.ShouldEqual, 1)
	err = col.FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	test.That(t, err, test.ShouldBeNil)
	return
}
