#!/usr/bin/env node
import * as path from 'path';
import * as es from '@aws-cdk/aws-elasticsearch';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as lambdanodejs from '@aws-cdk/aws-lambda-nodejs';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import * as destinations from '../lib';

const app = new cdk.App();

const stack = new cdk.Stack(app, 'delivery-stream-elasticsearch-destination');

const domain = new es.Domain(stack, 'Domain', {
  version: es.ElasticsearchVersion.V5_6,
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

const dataProcessorFunction = new lambdanodejs.NodejsFunction(stack, 'DataProcessorFunction', {
  entry: path.join(__dirname, 'lambda-data-processor.js'),
  timeout: cdk.Duration.minutes(1),
});

const processor = new firehose.LambdaFunctionProcessor(dataProcessorFunction, {
  bufferInterval: cdk.Duration.seconds(60),
  bufferSize: cdk.Size.mebibytes(1),
  retries: 1,
});

new firehose.DeliveryStream(stack, 'Delivery Stream', {
  destinations: [new destinations.ElasticsearchDomain(domain, {
    indexName: 'my_index',
    typeName: 'my_type',
    indexRotation: destinations.IndexRotationPeriod.ONE_MONTH,
    retryInterval: cdk.Duration.minutes(100),
    processor: processor,
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