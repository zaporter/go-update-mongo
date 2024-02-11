package update

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.viam.com/test"
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
func NewDecimal128(t *testing.T, v string) primitive.Decimal128 {
	t.Helper()
	val, err := primitive.ParseDecimal128(v)
	test.That(t, err, test.ShouldBeNil)
	return val
}

func TestBehaviorParity(t *testing.T) {
	type upT = map[string]interface{}
	tests := []struct {
		name             string
		object           bson.D
		update           map[string]interface{}
		shouldContainErr string
		skip             bool
	}{
		{
			name:             "empty to empty",
			object:           bson.D{},
			update:           map[string]interface{}{},
			shouldContainErr: "update document must have at least one element",
		},
		// ---------------------- Field Operators -----------------------------
		//
		// $set
		//
		{
			name:   "set field",
			object: bson.D{{"key", "val1"}},
			update: upT{
				"$set": upT{"key": "val2"},
			},
		},
		{
			name:   "set new field on empty doc",
			object: bson.D{},
			update: upT{
				"$set": upT{"key": "newval"},
			},
		},
		{
			name:   "set an array",
			object: bson.D{},
			update: upT{
				"$set": upT{"key": primitive.A{1, 3}},
			},
		},
		{
			name:   "update an array with set",
			object: bson.D{{"key", primitive.A{1, 2}}},
			update: upT{
				"$set": upT{"key": primitive.A{1, 3, 3}},
			},
		},
		{
			name:   "set swap int to array of strings",
			object: bson.D{{"key", 1}},
			update: upT{
				"$set": upT{"key": primitive.A{"hi", "foo", "bar"}},
			},
		},
		{
			name:   "update var and set new var",
			object: bson.D{{"key", "val1"}},
			update: upT{
				"$set": upT{"key": "newval", "newval": "k"},
			},
		},
		{
			name:   "set nested object",
			object: bson.D{{"key", bson.D{{"subkey", 1}}}},
			update: upT{
				"$set": upT{"key.subkey": 2},
			},
			skip: true,
		},
		{
			name:   "set inserts in the correct order",
			object: bson.D{},
			update: upT{
				"$set": upT{"2": "newval", "1": "k"},
			},
			skip: true,
		},
		//
		// $currentDate
		//
		{
			name:   "currentDate empty",
			object: bson.D{},
			update: upT{
				"$currentDate": upT{},
			},
			skip: true,
		},
		{
			name:   "currentDate missing field true",
			object: bson.D{},
			update: upT{
				"$currentDate": upT{"field": true},
			},
			skip: true,
		},
		{
			name:   "currentDate true",
			object: bson.D{{"field", 1}},
			update: upT{
				"$currentDate": upT{"field": true},
			},
			skip: true,
		},
		{
			name:   "currentDate nested set",
			object: bson.D{{"field", 1}},
			update: upT{
				"$currentDate": upT{"unknown.bar": true},
			},
			skip: true,
		},
		{
			name:   "currentDate document timestamp",
			object: bson.D{{"field", 1}},
			update: upT{
				"$currentDate": upT{"field": upT{"$type": "timestamp"}},
			},
			skip: true,
		},
		{
			name:   "currentDate document date",
			object: bson.D{{"field", 1}},
			update: upT{
				"$currentDate": upT{"field": upT{"$type": "date"}},
			},
			skip: true,
		},
		{
			name:   "currentDate document anything else",
			object: bson.D{{"field", 1}},
			update: upT{
				"$currentDate": upT{"field": upT{"$type": "foobar"}},
			},
			skip: true,
		},
		//
		// $inc
		//
		{
			name:   "inc unknown field",
			object: bson.D{{"field", 1}},
			update: upT{
				"$inc": upT{"field2": 1},
			},
			skip: true,
		},
		{
			name:   "inc known base field",
			object: bson.D{{"field", 1}},
			update: upT{
				"$inc": upT{"field": 2},
			},
			skip: true,
		},
		{
			name:   "inc zero",
			object: bson.D{{"field", 1}},
			update: upT{
				"$inc": upT{"field": 0},
			},
			skip: true,
		},
		{
			name:   "inc fraction",
			object: bson.D{{"field", 1}},
			update: upT{
				"$inc": upT{"field": 0.5},
			},
			skip: true,
		},
		{
			name:   "inc known base field negative",
			object: bson.D{{"field", 1}},
			update: upT{
				"$inc": upT{"field": -102},
			},
			skip: true,
		},
		{
			name:   "inc nested field",
			object: bson.D{{"field", bson.M{"nested": 1}}},
			update: upT{
				"$inc": upT{"field.nested": -102},
			},
			skip: true,
		},
		{
			name:   "inc negative nested field",
			object: bson.D{{"field", bson.M{"nested": -1}}},
			update: upT{
				"$inc": upT{"field.nested": -102},
			},
			skip: true,
		},
		{
			name:   "inc negative nested non-existent field",
			object: bson.D{{"field", bson.M{"nested": -1}}},
			update: upT{
				"$inc": upT{"field.bar": -102},
			},
			skip: true,
		},
		//
		// $mul
		//
		{
			name:   "mul unknown field",
			object: bson.D{{"field", 1}},
			update: upT{
				"$mul": upT{"field2": 1},
			},
			skip: true,
		},
		{
			name:   "mul known base field",
			object: bson.D{{"field", 1}},
			update: upT{
				"$mul": upT{"field": 2},
			},
			skip: true,
		},
		{
			name:   "mul zero",
			object: bson.D{{"field", 1}},
			update: upT{
				"$mul": upT{"field": 0},
			},
			skip: true,
		},
		{
			name:   "mul floats",
			object: bson.D{{"field", 1.5}},
			update: upT{
				"$mul": upT{"field": -1.3},
			},
			skip: true,
		},
		{
			name:   "mul small ints",
			object: bson.D{{"field", 5}},
			update: upT{
				"$mul": upT{"field": 5},
			},
			skip: true,
		},
		{
			name:   "mul one large int",
			object: bson.D{{"field", 3*10 ^ 9}},
			update: upT{
				"$mul": upT{"field": 3},
			},
			skip: true,
		},
		{
			name:   "mul two large ints",
			object: bson.D{{"field", 3*10 ^ 9}},
			update: upT{
				"$mul": upT{"field": 4*10 ^ 9},
			},
			skip: true,
		},
		{
			name:   "mul one large int and a float",
			object: bson.D{{"field", 3*10 ^ 9}},
			update: upT{
				"$mul": upT{"field": 1.2},
			},
			skip: true,
		},
		{
			name:   "mul Decimal128",
			object: bson.D{{"field", NewDecimal128(t, "123.123734")}},
			update: upT{
				"$mul": upT{"field": NewDecimal128(t, "-52.236")},
			},
			skip: true,
		},
		{
			name:   "mul fraction",
			object: bson.D{{"field", 1}},
			update: upT{
				"$mul": upT{"field": 0.5},
			},
			skip: true,
		},
		{
			name:   "mul known base field negative",
			object: bson.D{{"field", 1}},
			update: upT{
				"$mul": upT{"field": -102},
			},
			skip: true,
		},
		{
			name:   "mul nested field",
			object: bson.D{{"field", bson.M{"nested": 1}}},
			update: upT{
				"$mul": upT{"field.nested": -102},
			},
			skip: true,
		},
		{
			name:   "mul negative nested field",
			object: bson.D{{"field", bson.M{"nested": -1}}},
			update: upT{
				"$mul": upT{"field.nested": -102},
			},
			skip: true,
		},
		{
			name:   "mul negative nested non-existent field",
			object: bson.D{{"field", bson.M{"nested": -1}}},
			update: upT{
				"$mul": upT{"field.bar": -102},
			},
			skip: true,
		},
		//
		// $rename
		//
		{
			name:   "rename simple",
			object: bson.D{{"field", 1}},
			update: upT{
				"$rename": upT{"field": "alias"},
			},
			skip: true,
		},
		{
			name:   "rename simple missing",
			object: bson.D{{"field", 1}},
			update: upT{
				"$rename": upT{"fieldnope": "alias"},
			},
			skip: true,
		},
		{
			name:   "rename multiple",
			object: bson.D{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{
				"$rename": upT{"field": "a", "twin": "b", "map": "c"},
			},
			skip: true,
		},
		{
			name:   "rename nested down to first level",
			object: bson.D{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{
				"$rename": upT{"field.a": "a"},
			},
			skip: true,
		},
		{
			name:   "rename nested within same level",
			object: bson.D{{"field", 1}, {"twin", "blah"}, {"map", bson.M{"a": "b"}}},
			update: upT{
				"$rename": upT{"field.a": "field.chicken"},
			},
			skip: true,
		},
		// ---------------------- Bitwise Operators -----------------------------
		//
		// $bit
		//
		// TODO under-tested
		{
			name:   "bit and",
			object: bson.D{{"field", 13}},
			update: upT{
				"$bit": upT{"field": upT{"and": 10}},
			},
			skip: true,
		},
		{
			name:   "bit or",
			object: bson.D{{"field", 13}},
			update: upT{
				"$bit": upT{"field": upT{"or": 10}},
			},
			skip: true,
		},
		{
			name:   "bit xor",
			object: bson.D{{"field", 13}},
			update: upT{
				"$bit": upT{"field": upT{"xor": 10}},
			},
			skip: true,
		},
		// ---------------------- Array Operators -----------------------------
		//
		// $
		//
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
			insertRes, err := col.InsertOne(ctx, tc.object)
			test.That(t, err, test.ShouldBeNil)
			id := insertRes.InsertedID
			update, err := col.UpdateOne(ctx, bson.M{"_id": id}, tc.update)
			if tc.shouldContainErr != "" {
				test.That(t, fmt.Sprint(err), test.ShouldContainSubstring, tc.shouldContainErr)
				_, err := UpdateDocument(tc.object, tc.update)
				test.That(t, fmt.Sprint(err), test.ShouldContainSubstring, tc.shouldContainErr)
			} else {
				test.That(t, err, test.ShouldBeNil)
				test.That(t, update.ModifiedCount, test.ShouldEqual, 1)
				var output *bson.D
				err = col.FindOne(ctx, bson.M{"_id": id}).Decode(&output)
				// calc from UpdateDocuemnt
				tcObjectWithID := bson.D{{"_id", id}}
				for _, v := range tc.object {
					tcObjectWithID = append(tcObjectWithID, v)
				}
				actual, err := UpdateDocument(tcObjectWithID, tc.update)
				// marshal because equality with prims can be strange
				test.That(t, err, test.ShouldBeNil)
				jsonMongo, err := json.Marshal(output)
				test.That(t, err, test.ShouldBeNil)
				jsonMe, err := json.Marshal(actual)
				test.That(t, err, test.ShouldBeNil)
				test.That(t, string(jsonMongo), test.ShouldResemble, string(jsonMe))
			}
		})
	}
}
