import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration, Size } from '@aws-cdk/core';
import { Construct } from 'constructs';

/**
 * S3 destination of a delivery stream
 */
export interface S3DestinationProps extends firehose.DestinationProps {

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
   * A prefix that Kinesis Data Firehose evaluates and adds to failed records
   * before writing them to S3. This prefix appears immediately following the
   * bucket name.
   *
   * @default - TODO
   */
  readonly errorOutputPrefix?: string;

  /**
   * The type of compression that Kinesis Data Firehose uses to compress the data
   * that it delivers to the Amazon S3 bucket.
   *
   * The compression formats SNAPPY or ZIP cannot be specified for Amazon Redshift
   * destinations because they are not supported by the Amazon Redshift COPY operation
   * that reads from the S3 bucket.
   *
   * @default - UNCOMPRESSED
   */
  readonly compressionFormat?: firehose.Compression;

  /**
   *  The AWS KMS key used to encrypt the data that it delivers
   *  to your Amazon S3 bucket.
   *
   * @default - Data is not encrypted.
   */
  readonly encryptionKey?: kms.IKey;

  /**
   * The size of the buffer that Firehose uses for incoming data before
   * delivering it to the intermediate bucket.
   *
   * Minimum: Duration.seconds(60)
   * Maximum: Duration.seconds(900)
   *
   * @default Duration.seconds(60)
   */
  readonly bufferingInterval?: Duration;

  /**
   * The length of time that Firehose buffers incoming data before delivering
   * it to the intermediate bucket.
   *
   * Minimum: Size.mebibytes(1)
   * Maximum: Size.mebibytes(128)
   *
   * @default Size.mebibytes(3)
   */
  readonly bufferingSize?: Size;
}

/**
 * An S3 bucket destination for data from a Kinesis Firehose delivery stream.
 */
export class S3Destination extends firehose.DestinationBase {
  constructor(private readonly s3Props: S3DestinationProps) {
    super(s3Props);

    validateS3Props(s3Props);
  }

  bind(_scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
    this.s3Props.bucket.grantReadWrite(options.deliveryStream);

    return {
      properties: {
        s3DestinationConfiguration: {
          bucketArn: this.s3Props.bucket.bucketArn,
          roleArn: (options.deliveryStream.grantPrincipal as iam.IRole).roleArn,
          prefix: this.s3Props.prefix,
          errorOutputPrefix: this.s3Props.errorOutputPrefix,
          compressionFormat: this.s3Props.compressionFormat,
          encryptionConfiguration: createEncryptionConfig(this.s3Props.encryptionKey),
          bufferingHints: this.createBufferingHints(this.s3Props.bufferingInterval, this.s3Props.bufferingSize),
        },
      },
    };
  }
}

function createEncryptionConfig(encryptionKey?: kms.IKey): firehose.CfnDeliveryStream.EncryptionConfigurationProperty {
  return encryptionKey != null
    ? { kmsEncryptionConfig: { awskmsKeyArn: encryptionKey.keyArn } }
    : { noEncryptionConfig: 'NoEncryption' };
}

function validateS3Props(s3Props: S3DestinationProps) {
  const bufferingInterval = s3Props.bufferingInterval;
  if (bufferingInterval != null && (bufferingInterval.toSeconds() < 60 || bufferingInterval.toSeconds() > 900)) {
    throw new Error('Invalid bufferingInterval. Valid range: [60, 900]');
  }

  const bufferingSize = s3Props.bufferingSize;
  if (bufferingSize != null && (bufferingSize.toMebibytes() < 1 || bufferingSize.toMebibytes() > 128)) {
    throw new Error('Invalid bufferingSize. Valid range: [1, 128]');
  }
}