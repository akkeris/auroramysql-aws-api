package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/go-martini/martini"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
)

type provisionspec struct {
	Plan        string `json:"plan"`
	Billingcode string `json:"billingcode"`
}
type tagspec struct {
	Resource string `json:"resource"`
	Name     string `json:"name"`
	Value    string `json:"value"`
}
type dbspec struct {
	Username       string
	Password       string
	Endpoint       string
	ReaderEndpoint string
}

func printDBStats(db *sql.DB) {
	fmt.Printf("Open connections: %v\n", db.Stats().OpenConnections)
}

func getDB(t string, uri string) *sql.DB {
	db, dberr := sql.Open(t, uri)
	if dberr != nil {
		fmt.Println(dberr)
		return nil
	}
	// not available in 1.5 golang, youll want to turn it on for v1.6 or higher once upgraded.
	//pool.SetConnMaxLifetime(time.ParseDuration("1h"));
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(20)
	return db
}

var pool *sql.DB
var pool_hobby *sql.DB

func main() {
	if os.Getenv("REGION") == "" {
		fmt.Println("REGION was not specified.")
		os.Exit(2)
	}
	if os.Getenv("HOBBY_DB") == "" {
		fmt.Println("HOBBY_DB was not specifyied.")
		os.Exit(2)
	}
	if os.Getenv("BROKER_DB") == "" {
		fmt.Println("BROKER_DB was not specifyied.")
		os.Exit(2)
	}
	if strings.Index(os.Getenv("HOBBY_DB"), "@") == -1 {
		fmt.Println("HOBBY_DB was not a valid mysql db uri.  E.g., user:pass@tcp(host:3306)/db")
		os.Exit(2)
	}
	if os.Getenv("ENVIRONMENT") == "" || strings.Index(os.Getenv("ENVIRONMENT"), "-") > -1 || strings.Index(os.Getenv("ENVIRONMENT"), "_") > -1 {
		fmt.Println("ENVIORNMENT was not set or had an invalid character, it can only be alpha numeric.")
		os.Exit(2)
	}
	pool = getDB("postgres", os.Getenv("BROKER_DB"))
	pool_hobby = getDB("mysql", os.Getenv("HOBBY_DB"))
	m := martini.Classic()
	m.Use(render.Renderer())
	m.Post("/v1/aurora-mysql/instance", binding.Json(provisionspec{}), provision)
	m.Delete("/v1/aurora-mysql/instance/:name", delete)
	m.Get("/v1/aurora-mysql/plans", plans)
	m.Get("/v1/aurora-mysql/url/:name", url)
	m.Post("/v1/tag", binding.Json(tagspec{}), tag)
	m.Run()
}

func tag(spec tagspec, berr binding.Errors, r render.Render) {
	if berr != nil {
		fmt.Println(berr)
		errorout := make(map[string]interface{})
		errorout["error"] = berr
		r.JSON(500, errorout)
		return
	}
	fmt.Println(spec.Resource)
	fmt.Println(spec.Name)
	fmt.Println(spec.Value)
	svc := rds.New(session.New(&aws.Config{
		Region: aws.String(os.Getenv("REGION")),
	}))
	region := os.Getenv("REGION")
	accountnumber := os.Getenv("ACCOUNTNUMBER")
	name := spec.Resource

	arnname := "arn:aws:rds:" + region + ":" + accountnumber + ":db:" + name

	params := &rds.AddTagsToResourceInput{
		ResourceName: aws.String(arnname),
		Tags: []*rds.Tag{ // Required
			{
				Key:   aws.String(spec.Name),
				Value: aws.String(spec.Value),
			},
		},
	}
	resp, err := svc.AddTagsToResource(params)

	if err != nil {
		fmt.Println(err.Error())
		errorout := make(map[string]interface{})
		errorout["error"] = berr
		r.JSON(500, errorout)
		return
	}

	fmt.Println(resp)
	r.JSON(200, map[string]interface{}{"response": "tag added"})
}

func provision(spec provisionspec, err binding.Errors, r render.Render) {
	plan := spec.Plan
	billingcode := spec.Billingcode

	var name string
	dberr := pool.QueryRow("select name from aurora_mysql_provision where plan='" + plan + "' and claimed='no' and make_date=(select min(make_date) from aurora_mysql_provision where plan='" + plan + "' and claimed='no')").Scan(&name)
	if dberr != nil {
		fmt.Println(dberr)
		toreturn := dberr.Error()
		r.JSON(500, map[string]interface{}{"error": toreturn})
		return
	}
	fmt.Println(name)

	instanceName := name

	available := true
	if plan == "large" && !isAvailable(instanceName) {
		available = false
	}
	if available {
		var dbinfo dbspec
		stmt, dberr := pool.Prepare("update aurora_mysql_provision set claimed=$1 where name=$2")

		if dberr != nil {
			fmt.Println(dberr)
			toreturn := dberr.Error()
			r.JSON(500, map[string]interface{}{"error": toreturn})
			return
		}
		_, dberr = stmt.Exec("yes", name)
		if dberr != nil {
			fmt.Println(dberr)
			toreturn := dberr.Error()
			r.JSON(500, map[string]interface{}{"error": toreturn})
			return
		}

		if spec.Plan != "micro" && spec.Plan != "small" && spec.Plan != "medium" {
			region := os.Getenv("REGION")
			svc := rds.New(session.New(&aws.Config{
				Region: aws.String(region),
			}))
			accountnumber := os.Getenv("ACCOUNTNUMBER")
			arnname := "arn:aws:rds:" + region + ":" + accountnumber + ":cluster:" + name

			params := &rds.AddTagsToResourceInput{
				ResourceName: aws.String(arnname),
				Tags: []*rds.Tag{ // Required
					{
						Key:   aws.String("billingcode"),
						Value: aws.String(billingcode),
					},
				},
			}

			resp, awserr := svc.AddTagsToResource(params)
			if awserr != nil {
				fmt.Println(awserr.Error())
				toreturn := awserr.Error()
				r.JSON(500, map[string]interface{}{"error": toreturn})
				return
			}
			fmt.Println(resp)
		}

		dbinfo, err := getDBInfo(name)
		if err != nil {
			toreturn := err.Error()
			r.JSON(500, map[string]interface{}{"error": toreturn})
			return
		}

		r.JSON(200, map[string]string{"DATABASE_URL": dbinfo.Username + ":" + dbinfo.Password + "@" + dbinfo.Endpoint, "DATABASE_READONLY_URL": dbinfo.Username + ":" + dbinfo.Password + "@" + dbinfo.ReaderEndpoint})
		return
	}
	if !available {
		r.JSON(503, map[string]string{"DATABASE_URL": "", "DATABASE_READONLY_URL": ""})
		return
	}
}

func delete(params martini.Params, r render.Render) {
	name := params["name"]
	region := os.Getenv("REGION")

	var plan string
	dberr := pool.QueryRow("SELECT plan from aurora_mysql_provision where name='" + name + "'").Scan(&plan)
	if dberr != nil {
		fmt.Println(dberr)
		toreturn := dberr.Error()
		r.JSON(500, map[string]interface{}{"error": toreturn})
		return
	}
	fmt.Println(plan)

	if plan == "micro" || plan == "small" || plan == "medium" {
		_, err := pool_hobby.Exec("DROP DATABASE " + name)
		if err != nil {
			fmt.Println(err)
			toreturn := err.Error()
			r.JSON(500, map[string]interface{}{"error": toreturn})
			return
		}

	} else {
		svc := rds.New(session.New(&aws.Config{
			Region: aws.String(region),
		}))

		instparams := &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(name + "-" + region + "a"), // Required
			SkipFinalSnapshot:    aws.Bool(true),
		}
		instparams2 := &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(name + "-" + region + "b"), // Required
			SkipFinalSnapshot:    aws.Bool(true),
		}

		clusterparams := &rds.DeleteDBClusterInput{
			DBClusterIdentifier: aws.String(name),
			SkipFinalSnapshot:   aws.Bool(true),
		}

		_, derr := svc.DeleteDBInstance(instparams)
		if derr != nil {
			fmt.Println(derr.Error())
			errorout := make(map[string]interface{})
			errorout["error"] = derr.Error()
			r.JSON(500, errorout)
			return
		}
		_, derr2 := svc.DeleteDBInstance(instparams2)
		if derr2 != nil {
			fmt.Println(derr2.Error())
			errorout := make(map[string]interface{})
			errorout["error"] = derr2.Error()
			r.JSON(500, errorout)
			return
		}
		_, derr3 := svc.DeleteDBCluster(clusterparams)
		if derr3 != nil {
			fmt.Println(derr3.Error())
			errorout := make(map[string]interface{})
			errorout["error"] = derr3.Error()
			r.JSON(500, errorout)
			return
		}
	}

	fmt.Println("# Deleting")
	stmt, err := pool.Prepare("delete from aurora_mysql_provision where name=$1")
	if err != nil {
		errorout := make(map[string]interface{})
		errorout["error"] = err.Error()
		r.JSON(500, errorout)
		return
	}
	res, err := stmt.Exec(name)
	if err != nil {
		errorout := make(map[string]interface{})
		errorout["error"] = err.Error()
		r.JSON(500, errorout)
		return
	}
	affect, err := res.RowsAffected()
	if err != nil {
		errorout := make(map[string]interface{})
		errorout["error"] = err.Error()
		r.JSON(500, errorout)
		return
	}
	fmt.Println(affect, "rows changed")

	r.JSON(200, map[string]interface{}{"status": "deleted"})
}

func plans(r render.Render) {
	plans := make(map[string]interface{})
	plans["micro"] = "Shared Tenancy"
	plans["small"] = "2x CPU - 4GB Mem - 20GB Disk - Extra IOPS:no"
	plans["medium"] = "2x CPU - 8GB Mem - 50GB Disk - Extra IOPS:no"
	plans["large"] = " 4x CPU - 30GB Mem - 100GB Disk - Extra IOPS:1000"
	r.JSON(200, plans)
}

func url(params martini.Params, r render.Render) {
	name := params["name"]
	dbinfo, err := getDBInfo(name)
	if err != nil {
		toreturn := err.Error()
		r.JSON(500, map[string]interface{}{"error": toreturn})
		return
	}
	r.JSON(200, map[string]string{"DATABASE_URL": (dbinfo.Username + ":" + dbinfo.Password + "@" + dbinfo.Endpoint), "DATABASE_READONLY_URL": (dbinfo.Username + ":" + dbinfo.Password + "@" + dbinfo.ReaderEndpoint)})
}

func getDBInfo(name string) (dbinfo dbspec, err error) {
	dbinfo.Username = queryDB("masteruser", name)
	dbinfo.Password = queryDB("masterpass", name)
	dbinfo.Endpoint = queryDB("endpoint", name)
	dbinfo.ReaderEndpoint = queryDB("reader_endpoint", name)
	return dbinfo, nil
}

func queryDB(i string, name string) string {
	dberr := pool.QueryRow("select " + i + " from aurora_mysql_provision where name ='" + name + "'").Scan(&i)
	if dberr != nil {
		fmt.Println("Unable to fetch info ", dberr)
		return ""
	}
	fmt.Println(name)
	printDBStats(pool)
	return i
}

func isAvailable(name string) bool {
	var toreturn bool
	region := os.Getenv("REGION")

	svc := rds.New(session.New(&aws.Config{
		Region: aws.String(region),
	}))

	rparams := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(name),
		MaxRecords:          aws.Int64(20),
	}
	rresp, rerr := svc.DescribeDBClusters(rparams)
	if rerr != nil {
		fmt.Println(rerr)
	}
	//      fmt.Println(rresp)
	fmt.Println("Checking to see if available...")
	fmt.Println(*rresp.DBClusters[0].Status)
	status := *rresp.DBClusters[0].Status
	if status == "available" {
		toreturn = true
	}
	if status != "available" {
		toreturn = false
	}
	return toreturn
}
