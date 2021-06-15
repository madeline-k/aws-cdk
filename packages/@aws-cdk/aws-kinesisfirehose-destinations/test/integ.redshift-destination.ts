#!/usr/bin/env node
import * as ec2 from '@aws-cdk/aws-ec2';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as redshift from '@aws-cdk/aws-redshift';
import * as cdk from '@aws-cdk/core';
import * as constructs from 'constructs';
import * as firehosedestinations from '../lib';

/**
 * Stack verification steps:
 * data=`echo '{"ticker_symbol":"AMZN","sector":"TECHNOLOGY","change":1.32,"price":736.83}' | base64`
 * aws firehose put-record --delivery-stream-name <delivery-stream-name> --record "{\"Data\":\"$data\"}"
 */

const app = new cdk.App();

const stack = new cdk.Stack(app, 'aws-cdk-firehose-redshift-destination');
cdk.Aspects.of(stack).add({
  visit(node: constructs.IConstruct) {
    if (cdk.CfnResource.isCfnResource(node)) {
      node.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }
  },
});

const vpc = new ec2.Vpc(stack, 'Vpc');
const database = 'my_db';
const cluster = new redshift.Cluster(stack, 'Cluster', {
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

const redshiftDestination = new firehosedestinations.RedshiftDestination({
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
  bufferingInterval: cdk.Duration.minutes(1),
  bufferingSize: cdk.Size.mebibytes(1),
});
new firehose.DeliveryStream(stack, 'Firehose', {
  destination: redshiftDestination,
});

app.synth();
