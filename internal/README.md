Steps to update ferret internal dir:

0) `rm -rf ./ferret`
1) Copy the ferret internal dir into here (`cp -r /path/to/FerretDB/internal ./ferret`)
2) Run 
```sh
RecursiveReplace.sh "github.com/FerretDB/FerretDB/internal" "github.com/zaporter/go-update-mongo/internal/ferret"
```
3) Update the main README with the new commit of ferret you brought in and the time of the commit (not the current time)

