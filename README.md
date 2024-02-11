# go-update-mongo

Non applicable updates
[$setOnInsert](https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/) would do nothing because the doc is not inserted into anything... If anyone has a complelling reason why this should be a noop, I can add it as a noop.

[$(update)](https://www.mongodb.com/docs/manual/reference/operator/update/positional/) Does not make sense because there is no mongo query
