# Amazon Kinesis Firehose Destinations Library
<!--BEGIN STABILITY BANNER-->

---

![cdk-constructs: Experimental](https://img.shields.io/badge/cdk--constructs-experimental-important.svg?style=for-the-badge)

> The APIs of higher level constructs in this module are experimental and under active development.
> They are subject to non-backward compatible changes or removal in any future version. These are
> not subject to the [Semantic Versioning](https://semver.org/) model and breaking changes will be
> announced in the release notes. This means that while you may use them, you may need to update
> your source code when upgrading to a newer version of this package.

---

<!--END STABILITY BANNER-->

This library provides constructs for adding destinations to a Kinesis Firehose delivery stream.
Destinations can be added by specifying the `destination` prop when creating a delivery stream.

## Destinations

The following destinations are supported.

### Elasticsearch

Example with an Elasticsearch destination:

```ts
import * as es from '@aws-cdk/aws-elasticsearch';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';

const domain = new es.Domain(this, 'Domain', {
  version: es.ElasticsearchVersion.V7_1,
});

const deliveryStream = new firehose.DeliveryStream(this, 'DeliveryStream', {
  destination: new ElasticsearchDestination({
    domain: domain,
    indexName: 'myindex',
  }),
});
```

### Redshift

```ts
import * as ec2 from '@aws-cdk/aws-ec2';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as redshift from '@aws-cdk/aws-redshift';
import { Duration, Size } from '@aws-cdk/core';

// Given a Redshift cluster
const vpc = new ec2.Vpc(this, 'Vpc');
const database = 'my_db';
const cluster = new redshift.Cluster(this, 'Cluster', {
  vpc: vpc,
  vpcSubnets: {
    subnetType: ec2.SubnetType.PUBLIC,
  },
  masterUser: {
    masterUsername: 'master',
  },
  defaultDatabaseName: database,
  publiclyAccessible: true,
});

const redshiftDestination = new RedshiftDestination({
  cluster: cluster,
  user: {
    username: 'firehose',
  },
  database: database,
  tableName: 'firehose_test_table',
  tableColumns: [
    { name: 'TICKER_SYMBOL', dataType: 'varchar(4)' },
    { name: 'SECTOR', dataType: 'varchar(16)' },
    { name: 'CHANGE', dataType: 'float' },
    { name: 'PRICE', dataType: 'float' },
  ],
  copyOptions: 'json \'auto\'',
  bufferingInterval: Duration.minutes(1),
  bufferingSize: Size.mebibytes(1),
});
new firehose.DeliveryStream(this, 'Firehose', {
  destination: redshiftDestination,
});
```
