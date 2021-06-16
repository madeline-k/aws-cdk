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
   * TODO Add doc
   *
   * @default - TODO
   */
  readonly compressionFormat?: firehose.Compression;

  /**
   *  The AWS KMS key used to encrypt the data that it delivers
   *  to your Amazon S3 bucket
   *
   * @default - TODO
   */
  readonly encryptionKey?: kms.IKey;

  /**
   * The size of the buffer that Firehose uses for incoming data before delivering it to the intermediate bucket.
   *
   * TODO: valid values [60, 900] seconds
   *
   * @default Duration.seconds(60)
   */
  readonly bufferingInterval?: Duration;

  /**
   * The length of time that Firehose buffers incoming data before delivering it to the intermediate bucket.
   *
   * TODO: valid values [1, 128] MBs
   *
   * @default Size.mebibytes(3)
   */
  readonly bufferingSize?: Size;
}

/**
 * TODO Add doc
 */
export class S3Destination extends firehose.DestinationBase {
  constructor(private readonly s3Props: S3DestinationProps) {
    super(s3Props);
  }

  /**
   * TODO Add doc
   */
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