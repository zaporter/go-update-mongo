# go-update-mongo

Non applicable updates
[$setOnInsert](https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/) would do nothing because the doc is not inserted into anything... If anyone has a complelling reason why this should be a noop, I can add it as a noop.

[$(update)](https://www.mongodb.com/docs/manual/reference/operator/update/positional/) Does not make sense because there is no mongo query


# Ferret
Commit: 282c8e16c458537758fb7f4c64c614c84df45ba3 
Commit time: Thu Feb 8 10:09:09 2024 +0400
Steps to bring in a new version are avaiable at `internal/README.md`
