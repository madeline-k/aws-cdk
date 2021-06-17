import '@aws-cdk/assert-internal/jest';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { Duration, Size } from '@aws-cdk/core';
import * as firehosedestinations from '../lib';

describe('s3 destination', () => {
  let stack: cdk.Stack;
  let bucket: s3.IBucket;

  beforeEach(() => {
    stack = new cdk.Stack();
    bucket = new s3.Bucket(stack, 'destination');
  });

  it('rejects when bufferingInterval is too short', () => {
    expect(() => new firehosedestinations.S3Destination({
      bucket,
      bufferingInterval: Duration.seconds(59),
    })).toThrowError('Invalid bufferingInterval. Valid range: [60, 900]');
  });

  it('rejects when bufferingInterval is too long', () => {
    expect(() => new firehosedestinations.S3Destination({
      bucket,
      bufferingInterval: Duration.seconds(901),
    })).toThrowError('Invalid bufferingInterval. Valid range: [60, 900]');
  });

  it('rejects when bufferingSize is too short', () => {
    expect(() => new firehosedestinations.S3Destination({
      bucket,
      bufferingSize: Size.mebibytes(0),
    })).toThrowError('Invalid bufferingSize. Valid range: [1, 128] seconds');
  });

  it('rejects when bufferingSize is too long', () => {
    expect(() => new firehosedestinations.S3Destination({
      bucket,
      bufferingSize: Size.mebibytes(129),
    })).toThrowError('Invalid bufferingSize. Valid range: [1, 128] MiB');
  });
});