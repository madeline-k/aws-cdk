import * as iam from '@aws-cdk/aws-iam';
import * as logs from '@aws-cdk/aws-logs';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration, Size } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { IDeliveryStream } from './delivery-stream';
import { CfnDeliveryStream } from './kinesisfirehose.generated';
import { IDataProcessor } from './processor';

/**
 * A delivery stream destination configuration
 */
export interface DestinationConfig {
  /**
   * Schema-less properties that will be injected directly into `CfnDeliveryStream`.
   */
  readonly properties: object;
}

/**
 * Options when binding a destination to a delivery stream
 */
export interface DestinationBindOptions {
  /**
   * The delivery stream.
   */
  readonly deliveryStream: IDeliveryStream;
}

/**
 * A Kinesis Data Firehose Delivery Stream destination
 */
export interface IDestination {
  /**
   * Binds this destination to the Kinesis Data Firehose delivery stream
   *
   * Implementers should use this method to bind resources to the stack and initialize values using the provided stream.
   */
  bind(scope: Construct, options: DestinationBindOptions): DestinationConfig;
}

/**
 * Options for S3 record backup of a delivery stream
 */
export enum BackupMode {
  /**
   * All records are backed up.
   */
  ALL,

  /**
   * Only records that failed to deliver or transform are backed up.
   */
  FAILED,

  /**
   * No records are backed up.
   */
  DISABLED
}

/**
 * Possible compression options Kinesis Data Firehose can use to compress data on delivery
 */
export enum Compression {
  /**
   * gzip
   */
  GZIP = 'GZIP',

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
  ZIP = 'ZIP'
}

/**
 * Generic properties for defining a delivery stream destination
 */
export interface DestinationProps {
  /**
   * If true, log errors when Lambda invocation for data transformation or data delivery fails.
   *
   * If `logGroup` is provided, this will be implicitly set to `true`.
   *
   * @default true - errors are logged.
   */
  readonly logging?: boolean;

  /**
   * The CloudWatch log group where log streams will be created to hold error logs.
   *
   * @default - if `logging` is set to `true`, a log group will be created for you.
   */
  readonly logGroup?: logs.ILogGroup;

  /**
   * The series of data transformations that should be performed on the data before writing to the destination.
   *
   * TODO: add connection to Lambda VPC from fixed Firehose CIDR
   *
   * @default [] - no data transformation will occur.
   */
  readonly processors?: IDataProcessor[];

  /**
   * Indicates the mode by which incoming records should be backed up to S3, if any.
   *
   * If `backupBucket ` is provided, this will be implicitly set to `BackupMode.ALL`.
   *
   * @default BackupMode.DISABLED - source records are not backed up to S3.
   */
  readonly backup?: BackupMode;

  /**
   * The S3 bucket that will store data and failed records.
   *
   * @default - if `backup` is set to `BackupMode.ALL` or `BackupMode.FAILED`, a bucket will be created for you.
   */
  readonly backupBucket?: s3.IBucket;

  /**
   * The prefix Kinesis Data Firehose will prepend to all source records backed up to S3.
   *
   * @default 'source'
   */
  readonly backupPrefix?: string;

  // TODO: add backupBufferInterval and backupBufferSize
}

/**
 * Abstract base class that destination types can extend to benefit from methods that create generic configuration.
 */
export abstract class DestinationBase implements IDestination {
  private logGroup?: logs.ILogGroup;

  constructor(protected readonly props: DestinationProps = {}) {}

  abstract bind(scope: Construct, options: DestinationBindOptions): DestinationConfig;

  protected createLoggingOptions(
    scope: Construct,
    deliveryStream: IDeliveryStream,
    streamId: string,
  ): CfnDeliveryStream.CloudWatchLoggingOptionsProperty | undefined {
    if (this.props.logging === false && this.props.logGroup) {
      throw new Error('Destination logging cannot be set to false when logGroup is provided');
    }
    if (this.props.logging !== false || this.props.logGroup) {
      this.logGroup = this.logGroup ?? this.props.logGroup ?? new logs.LogGroup(scope, 'Log Group');
      this.logGroup.grantWrite(deliveryStream); // TODO: too permissive? add a new grant on the stream resource?
      return {
        enabled: true,
        logGroupName: this.logGroup.logGroupName,
        logStreamName: this.logGroup.addStream(streamId).logStreamName, // TODO: probably concatenate the stream ID with the construct node ID so conflicts don't occur
      };
    }
    return undefined;
  }

  protected createProcessingConfig(deliveryStream: IDeliveryStream): CfnDeliveryStream.ProcessingConfigurationProperty | undefined {
    if (this.props.processors && this.props.processors.length > 1) {
      throw new Error('Only one processor is allowed per delivery stream destination');
    }
    if (this.props.processors && this.props.processors.length > 0) {
      const processors = this.props.processors.map((processor) => {
        const processorConfig = processor.bind(deliveryStream);
        const parameters = [{ parameterName: 'RoleArn', parameterValue: (deliveryStream.grantPrincipal as iam.Role).roleArn }];
        parameters.push(processorConfig.processorIdentifier);
        if (processorConfig.bufferInterval) {
          parameters.push({ parameterName: 'BufferIntervalInSeconds', parameterValue: processorConfig.bufferInterval.toSeconds().toString() });
        }
        if (processorConfig.bufferSize) {
          // TODO: validate buffer size < 6MB due to Lambda synchronous invocation request/response size limits
          parameters.push({ parameterName: 'BufferSizeInMBs', parameterValue: processorConfig.bufferSize.toMebibytes().toString() });
        }
        if (processorConfig.retries) {
          parameters.push({ parameterName: 'NumberOfRetries', parameterValue: processorConfig.retries.toString() });
        }
        return {
          type: processorConfig.processorType,
          parameters: parameters,
        };
      });
      return {
        enabled: true,
        processors: processors,
      };
    }
    return undefined;
  }

  protected createBackupConfig(scope: Construct, deliveryStream: IDeliveryStream): CfnDeliveryStream.S3DestinationConfigurationProperty | undefined {
    if (this.props.backup === BackupMode.DISABLED && this.props.backupBucket) {
      throw new Error('Destination backup cannot be set to DISABLED when backupBucket is provided');
    }
    if ((this.props.backup !== undefined && this.props.backup !== BackupMode.DISABLED) || this.props.backupBucket) {
      const bucket = this.props.backupBucket ?? new s3.Bucket(scope, 'Backup Bucket');
      return {
        bucketArn: bucket.bucketArn,
        roleArn: (deliveryStream.grantPrincipal as iam.Role).roleArn,
        prefix: this.props.backupPrefix,
      };
    }
    return undefined;
  }

  protected createBufferingHints(bufferingInterval?: Duration, bufferingSize?: Size): CfnDeliveryStream.BufferingHintsProperty | undefined {
    if (!bufferingInterval && !bufferingSize) {
      return undefined;
    }
    if (bufferingInterval && (bufferingInterval.toSeconds() < 60 || bufferingInterval.toSeconds() > 900)) {
      throw new Error('Buffering interval must be between 1 and 15 minutes');
    }
    if (bufferingSize && (bufferingSize.toMebibytes() < 1 || bufferingSize.toMebibytes() > 128)) {
      throw new Error('Buffering size must be between 1 and 128 MBs');
    }
    return {
      intervalInSeconds: bufferingInterval?.toSeconds(),
      sizeInMBs: bufferingSize?.toMebibytes(),
    };
  }
}
