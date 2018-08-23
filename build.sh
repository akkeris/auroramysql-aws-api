#!/bin/sh

cd /app
go get  "github.com/lib/pq"
go get  "github.com/aws/aws-sdk-go/aws"
go get  "github.com/aws/aws-sdk-go/aws/session"
go get  "github.com/aws/aws-sdk-go/service/rds"
go get  "github.com/go-martini/martini"
go get  "github.com/martini-contrib/binding"
go get  "github.com/martini-contrib/render"
go get  "github.com/go-sql-driver/mysql"
go build server.go

