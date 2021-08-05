import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import { Construct } from 'constructs';
import { BackupMode, CommonDestinationProps, CommonDestinationS3Props, DestinationLoggingProps } from './common';
import { createBackupConfig, createBufferingHints, createEncryptionConfig, createLoggingOptions, createProcessingConfig } from './private/helpers';

/**
 * Properties for configuring the S3 backup an S3 delivery stream destination.
 */
export interface S3DestinationS3BackupProps extends DestinationLoggingProps, CommonDestinationS3Props {
  /**
   * The S3 bucket that will store data and failed records.
   *
   * @default - If `backup` is set to `BackupMode.ALL` or `BackupMode.FAILED`, a bucket will be created for you.
   */
  readonly bucket?: s3.IBucket;

  /**
   * Indicates the mode by which incoming records should be backed up to S3, if any.
   *
   * If `backupBucket ` is provided, this will be implicitly set to `BackupMode.ALL`.
   *
   * @default - If `backupBucket` is provided, the default will be `BackupMode.ALL`. Otherwise,
   * source records are not backed up to S3.
  */
  readonly mode?: BackupMode;
}

/**
 * Props for defining an S3 destination of a Kinesis Data Firehose delivery stream.
 */
export interface S3BucketProps extends CommonDestinationS3Props, CommonDestinationProps {
  /**
   * The configuration for backing up source records to S3.
   *
   * @default - source records will not be backed up to S3.
   */
  readonly s3Backup?: S3DestinationS3BackupProps;
}

/**
 * An S3 bucket destination for data from a Kinesis Data Firehose delivery stream.
 */
export class S3Bucket implements firehose.IDestination {
  constructor(private readonly bucket: s3.IBucket, private readonly props: S3BucketProps = {}) {
    if (this.props.s3Backup?.mode === BackupMode.FAILED) {
      throw new Error('S3 destinations do not support BackupMode.FAILED');
    }
  }

  bind(scope: Construct, _options: firehose.DestinationBindOptions): firehose.DestinationConfig {
    const role = this.props.role ?? new iam.Role(scope, 'S3 Destination Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    const bucketGrant = this.bucket.grantReadWrite(role);

    const { loggingOptions, dependables: loggingDependables } = createLoggingOptions(scope, {
      logging: this.props.logging,
      logGroup: this.props.logGroup,
      role,
      streamId: 'S3Destination',
    }) ?? {};

    const { backupConfig, dependables: backupDependables } = createBackupConfig(scope, role, this.props.s3Backup) ?? {};
    return {
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: loggingOptions,
        processingConfiguration: createProcessingConfig(scope, role, this.props.processor),
        roleArn: role.roleArn,
        s3BackupConfiguration: backupConfig,
        s3BackupMode: this.getS3BackupMode(),
        bufferingHints: createBufferingHints(this.props.bufferingInterval, this.props.bufferingSize),
        bucketArn: this.bucket.bucketArn,
        compressionFormat: this.props.compression?.value,
        encryptionConfiguration: createEncryptionConfig(role, this.props.encryptionKey),
        errorOutputPrefix: this.props.errorOutputPrefix,
        prefix: this.props.dataOutputPrefix,
      },
      dependables: [bucketGrant, ...(loggingDependables ?? []), ...(backupDependables ?? [])],
    };
  }

  private getS3BackupMode(): string | undefined {
    return this.props.s3Backup?.bucket || this.props.s3Backup?.mode === BackupMode.ALL
      ? 'Enabled'
      : undefined;
  }
}
