import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration, Size } from '@aws-cdk/core';

/**
 * Possible compression options Firehose can use to compress data on delivery
 *
 * TODO: I think we will need to move this to the main firehose module, but for
 * now it is only being used by the S3 destination. so leaving it here for now.
 */
export enum Compression {
  /**
   * gzip
   */
  GZIP= 'GZIP',

  /**
   * Hadoop-compatible Snappy
   */
  HADOOP_SNAPPY = 'HADOOP_SNAPPY',

  /**
   * Snappy
   */
  SNAPPY = 'Snappy',

  /**
   * Uncompressed
   */
  UNCOMPRESSED = 'UNCOMPRESSED',

  /**
   * ZIP
   */
  ZIP = 'ZIP',
}

/**
 * Specification of how Kinesis Data Firehose buffers incoming
 * data before delivering it to the destination.
 *
 * * TODO: I think we will need to move this to the main firehose module, but for
 * now it is only being used by the S3 destination. so leaving it here for now.
 */
export interface BufferingHints {
  /**
   * TODO Add doc
   */
  readonly interval: Duration,

  /**
   * TODO Add doc
   */
  readonly size: Size,
}

/**
 * S3 destination of a delivery stream
 */
export interface S3DestinationProps {

  /**
   * The bucket where the data will be stored
   */
  readonly bucket: s3.IBucket;

  /**
   * A prefix that Kinesis Data Firehose adds to the files that it delivers
   * to the Amazon S3 bucket. The prefix helps you identify the files that
   * Kinesis Data Firehose delivered.
   *
   * @default - TODO
   */
  readonly prefix?: string;

  /**
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly errorOutputPrefix?: string;

  /**
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly compressionFormat?: Compression;

  /**
   *  The AWS KMS key used to encrypt the data that it delivers
   *  to your Amazon S3 bucket
   *
   * @default - TODO
   */
  readonly encryptionKey?: kms.IKey;

  /**
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly bufferingHints?: BufferingHints;
}

/**
 * TODO Add doc
 */
export class S3Destination implements firehose.IDeliveryStreamDestination {
  constructor(private readonly props: S3DestinationProps) {
  }

  /**
   * TODO Add doc
   */
  bind(options: firehose.DeliveryStreamDestinationBindOptions):
  firehose.DeliveryStreamDestinationConfig {
    this.props.bucket.grantReadWrite(options.role);

    return {
      properties: {
        s3DestinationConfiguration: {
          bucketArn: this.props.bucket.bucketArn,
          roleArn: options.role.roleArn,
          prefix: this.props.prefix,
          errorOutputPrefix: this.props.errorOutputPrefix,
          compressionFormat: this.props.compressionFormat,
          encryptionConfiguration: encryptionConfigurationProperty(this.props.encryptionKey),
          bufferingHints: bufferingHintsProperty(this.props.bufferingHints),
        },
      },
    };
  }
}

function encryptionConfigurationProperty(encryptionKey?: kms.IKey): firehose.CfnDeliveryStream.EncryptionConfigurationProperty {
  return encryptionKey != null
    ? { kmsEncryptionConfig: { awskmsKeyArn: encryptionKey.keyArn } }
    : { noEncryptionConfig: 'NoEncryption' };
}

function bufferingHintsProperty(bufferingHints?: BufferingHints): firehose.CfnDeliveryStream.BufferingHintsProperty | undefined {
  if (bufferingHints != null) {
    return {
      sizeInMBs: bufferingHints?.size.toMebibytes(),
      intervalInSeconds: bufferingHints?.interval.toSeconds(),
    };
  }

  return undefined;
}