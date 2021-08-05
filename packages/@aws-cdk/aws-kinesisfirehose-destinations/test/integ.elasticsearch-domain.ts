#!/usr/bin/env node
import * as es from '@aws-cdk/aws-elasticsearch';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import * as destinations from '../lib';

const app = new cdk.App();

const stack = new cdk.Stack(app, 'aws-cdk-firehose-delivery-stream-s3-all-properties');

const domain = new es.Domain(stack, 'Domain', {
  version: es.ElasticsearchVersion.V7_9,
});

const backupBucket = new s3.Bucket(stack, 'BackupBucket', {
  removalPolicy: cdk.RemovalPolicy.DESTROY,
  autoDeleteObjects: true,
});
const logGroup = new logs.LogGroup(stack, 'LogGroup', {
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

const backupKey = new kms.Key(stack, 'BackupKey', {
  removalPolicy: cdk.RemovalPolicy.DESTROY,
});

new firehose.DeliveryStream(stack, 'Delivery Stream', {
  destinations: [new destinations.ElasticsearchDomain(domain, {
    indexName: 'my index',
    logging: true,
    logGroup: logGroup,
    bufferingInterval: cdk.Duration.seconds(60),
    bufferingSize: cdk.Size.mebibytes(1),
    s3Backup: {
      mode: destinations.BackupMode.ALL,
      bucket: backupBucket,
      compression: destinations.Compression.ZIP,
      dataOutputPrefix: 'backupPrefix',
      errorOutputPrefix: 'backupErrorPrefix',
      bufferingInterval: cdk.Duration.seconds(60),
      bufferingSize: cdk.Size.mebibytes(1),
      encryptionKey: backupKey,
    },
  })],
});

app.synth();