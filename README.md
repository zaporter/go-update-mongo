# Go-Update-Mongo

Go-Update-Mongo is a small go library that wraps the [FerretDB internals](https://github.com/FerretDB/FerretDB/) to provide a UpdateDocument function:
```golang
func UpdateDocument(document, updateDoc bson.D) (updatedDocument bson.D, err error) {}
```

The goal of this is to allow applications to perform complex operations on their data through mongo update operations rather than through functions. This is rarely better than a custom update function, however, if you want users to be able to update data on your platform, go-update-mongo allows you to accept user-input in the form of mongo update operations and run them in-memory rather than in a mdb database.

# Current failure areas:

[$(update)](https://www.mongodb.com/docs/manual/reference/operator/update/positional/) Unimplemented in FerretDB

[$\[\]](https://www.mongodb.com/docs/manual/reference/operator/update/positional-all/) Unimplemented in FerretDB

[$\<identifier\>](https://www.mongodb.com/docs/manual/reference/operator/update/positional-filtered/) FerretDB doesn't support ArrayFilters yet

[$position](https://www.mongodb.com/docs/manual/reference/operator/update/position/), [$slice](https://www.mongodb.com/docs/manual/reference/operator/update/slice/), [$sort](https://www.mongodb.com/docs/manual/reference/operator/update/sort/) They don't break the query but they don't work perfectly either. [$position](https://www.mongodb.com/docs/manual/reference/operator/update/position/) also has trouble with negative values


[$setOnInsert](https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/) partially works, however it is untested because I have a hard time determining exactly what the correct behavior should be here (because we are always performing an upsert action)

# Testing Methodology

Testing is performed by inserting an object in mongo, running `updateOne()` on it, and verifying that the document is identical to the document produced by `UpdateDocument`

There are currently 205 tests and 61 are skipped.

It is worth it to scan through `update/lib_test.go` to determine if your use case can be satisfied with the library at this point in time


# Running tests locally

Run `make up-test-mongo` to start a local mongodb docker container that the tests will run against. Then, run `go test ./update/... --count 1` to run the tests.


# Ferret Internals
```
Commit: 282c8e16c458537758fb7f4c64c614c84df45ba3 
Commit time: Thu Feb 8 10:09:09 2024 +0400
```
Steps to bring in a new version are avaiable at `internal/README.md`

Their tests do not pass in this repo because I have not set up the infra for their e2e tests
