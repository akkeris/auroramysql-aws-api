## Synopsis

Docker image which runs an http server with REST interface for provisioning of mysql aurora instances on AWS RDS

## Details

Listens on Port 3000
Supports the following

1. GET /v1/aurora-mysql/plans
2. POST /v1/aurora-mysql/instance/ with JSON data of plan and billingcode
3. DELETE /v1/aurora-mysql/intance/:name
4. GET /v1/aurora-mysql/url/:name


## Dependencies

1. "github.com/go-martini/martini"
2. "github.com/martini-contrib/render"
3. "github.com/martini-contrib/binding"
4. "github.com/aws/aws-sdk-go/aws"
5. "github.com/aws/aws-sdk-go/aws/session"
6. "github.com/aws/aws-sdk-go/service/rds"
7. "fmt"
8. "strconv"
9. "database/sql"
10. "github.com/lib/pq"
11. "github.com/go-sql-driver/mysql"
12. "os"



## Requirements
go

aws creds

## Runtime Environment Variables
1. ACCOUNT_NUMBER
2. BROKERDB
3. REGION


## Examples
`curl hostname:3000/v1/aurora-mysql/plans`

returns:
`
{
  "large": " 4x CPU - 30GB Mem - 100GB Disk - Extra IOPS:1000",
  "medium": "2x CPU - 8GB Mem - 50GB Disk - Extra IOPS:no",
  "small": "2x CPU - 4GB Mem - 20GB Disk - Extra IOPS:no"
}
`

`curl -X POST -d '{"plan":"small","billingcode":"code"}' hostname:3000/v1/postgres/instance`

returns
`{"DATABASE_URL":"postgres://username:password@db-name.uid.region.rds.amazonaws.com:5432/db-name"}`

`curl hostname:3000/v1/postgres/url/db-name`

returns `{"DATABASE_URL":"postgres://username:password@db-name.uid.region.rds.amazonaws.com:5432/db-name"}`


`curl -X DELETE hostname:3000/v1/postgres/instance/db-name` 

returns
`
{"DBInstance":{"AllocatedStorage":20,"AutoMinorVersionUpgrade":true,"AvailabilityZone":"us-west-2b","BackupRetentionPeriod":1,"CACertificateIdentifier":"rds-ca-2015","CharacterSetName":null,"CopyTagsToSnapshot":false,"DBClusterIdentifier":null,"DBInstanceClass":"db.t2.medium","DBInstanceIdentifier":"identifier","DBInstanceStatus":"deleting","DBName":"name","DBParameterGroups":[{"DBParameterGroupName":"rds-postgres-small","ParameterApplyStatus":"in-sync"}],"DBSecurityGroups":null,"DBSubnetGroup":{"DBSubnetGroupDescription":"subnet group description","DBSubnetGroupName":"subnetgroupname","SubnetGroupStatus":"Complete","Subnets":[{"SubnetAvailabilityZone":{"Name":"us-west-2b"},"SubnetIdentifier":"subnetid","SubnetStatus":"Active"},{"SubnetAvailabilityZone":{"Name":"us-west-2a"},"SubnetIdentifier":"subnetid","SubnetStatus":"Active"}],"VpcId":"vpcid"},"DbInstancePort":0,"DbiResourceId":"dbirid","DomainMemberships":null,"Endpoint":{"Address":"dbname.uid.region.rds.amazonaws.com","HostedZoneId":null,"Port":5432},"Engine":"postgres","EngineVersion":"9.5.2","EnhancedMonitoringResourceArn":null,"InstanceCreateTime":"2016-05-28T19:32:01.289Z","Iops":null,"KmsKeyId":null,"LatestRestorableTime":"2016-05-28T19:44:30Z","LicenseModel":"postgresql-license","MasterUsername":"username","MonitoringInterval":0,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupMemberships":[{"OptionGroupName":"default:postgres-9-5","Status":"in-sync"}],"PendingModifiedValues":{"AllocatedStorage":null,"BackupRetentionPeriod":null,"CACertificateIdentifier":null,"DBInstanceClass":null,"DBInstanceIdentifier":null,"EngineVersion":null,"Iops":null,"MasterUserPassword":null,"MultiAZ":null,"Port":null,"StorageType":null},"PreferredBackupWindow":"08:31-09:01","PreferredMaintenanceWindow":"tue:06:39-tue:07:09","PromotionTier":null,"PubliclyAccessible":false,"ReadReplicaDBInstanceIdentifiers":null,"ReadReplicaSourceDBInstanceIdentifier":null,"SecondaryAvailabilityZone":null,"StatusInfos":null,"StorageEncrypted":false,"StorageType":"gp2","TdeCredentialArn":null,"VpcSecurityGroups":[{"Status":"active","VpcSecurityGroupId":"vpcsecurityid"}]}}
`
