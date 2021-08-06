import * as es from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { BackupMode, CommonDestinationProps, CommonDestinationS3Props, DestinationBufferingProps, DestinationLoggingProps } from './common';
import { createBackupConfig, createBufferingHints, createLoggingOptions, createProcessingConfig } from './private/helpers';

/**
 * Possible index rotation periods for Elasticsearch index rotation.
 */
export enum IndexRotationPeriod {
  /**
   * No rotation
   */
  NO_ROTATION = 'NoRotation',
  /**
   * One hour
   */
  ONE_HOUR = 'OneHour',
  /**
   * One day
   */
  ONE_DAY = 'OneDay',
  /**
   * One week
   */
  ONE_WEEK = 'OneWeek',
  /**
   * One month
   */
  ONE_MONTH = 'OneMonth',
}

/**
 * Properties for configuring the S3 backup for an Elasticsearch delivery stream destination.
 */
export interface ElasticsearchDestinationS3BackupProps extends CommonDestinationS3Props, DestinationLoggingProps {
  /**
   * The S3 bucket that will store data and failed records.
   *
   * @default - A bucket will be created for you.
   */
  readonly bucket?: s3.IBucket;

  /**
   * Defines how documents should be delivered to Amazon S3. When it is set to `BackupMode.FAILED`, Kinesis Data Firehose writes
   * any documents that could not be indexed to the configured Amazon S3 destination, with elasticsearch-failed/
   * appended to the key prefix. When set to `BackupMode.ALL`, Kinesis Data Firehose delivers all incoming records to Amazon S3,
   * and also writes failed documents with elasticsearch-failed/ appended to the prefix. For more information, see Amazon S3 Backup for the Amazon ES Destination.
   *
   * @default - `BackupMode.FAILED`
   */
  readonly mode?: BackupMode;
}

/**
 * Props for defining an S3 destination of a Kinesis Data Firehose delivery stream.
 */
export interface ElasticsearchDomainProps extends CommonDestinationProps, DestinationLoggingProps, DestinationBufferingProps {
  /**
   * The configuration for backing up source records to S3.
   *
   * @default - An S3 bucket will be created for you. And only failed source records will be backed up.
   */
  readonly s3Backup?: ElasticsearchDestinationS3BackupProps;

  /**
   * After an initial failure to deliver to Amazon ES, the total amount of time during which
   * Kinesis Data Firehose re-attempts delivery (including the first attempt). If Kinesis Data Firehose
   * can't deliver the data within the specified time, it writes the data to the backup S3 bucket.
   *
   * A value of Duration.seconds(0) results in no retries.
   *
   * Minimum: Duration.seconds(0)
   * Maximum: Duration.seconds(7200)
   *
   * @default - Duration.seconds(300)
   */
  readonly retryInterval?: Duration;

  /**
   * The name of the Elasticsearch index to which Kinesis Data Firehose adds data for indexing.
   *
   * Elasticsearch index name must be lower-case, must not begin with an underscore, and must not contain commas.
   */
  readonly indexName: string;

  /**
   * The frequency of Elasticsearch index rotation. If you enable index rotation, Kinesis Data Firehose
   * appends a portion of the UTC arrival timestamp to the specified index name, and rotates the appended timestamp accordingly.
   *
   * For more information, see [Index Rotation for the Amazon ES Destination](https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html#es-index-rotation).
   *
   * @default - IndexRotationPeriod.ONE_DAY
   */
  readonly indexRotation?: IndexRotationPeriod;

  /**
   * The Elasticsearch type name that Amazon ES adds to documents when indexing data.
   *
   * @default - no type name is added
   */
  readonly typeName?: string;
}

/**
 * An Elasticsearch domain destination for data from a Kinesis Data Firehose delivery stream.
 */
export class ElasticsearchDomain implements firehose.IDestination {

  constructor(private readonly domain: es.IDomain, private readonly props: ElasticsearchDomainProps) {
    if (this.props.retryInterval?.toSeconds() && this.props.retryInterval.toSeconds() > 7200) {
      throw new Error('retry interval too big');
    };
    this.validateIndexName();
  }

  bind(scope: Construct, _options: firehose.DestinationBindOptions): firehose.DestinationConfig {
    const role = this.props.role ?? new iam.Role(scope, 'Elasticsearch Destination Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    this.domain.grantReadWrite(role);
    role.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'es:DescribeElasticsearchDomain',
        'es:DescribeElasticsearchDomains',
        'es:DescribeElasticsearchDomainConfig',
      ],
      resources: [
        this.domain.domainArn,
        this.domain.domainArn + '/*',
      ],
    }));

    const { loggingOptions, dependables: loggingDependables } = createLoggingOptions(scope, {
      logging: this.props.logging,
      logGroup: this.props.logGroup,
      role,
      streamId: 'ElasticsearchDestination',
    }) ?? {};

    let backupProps: ElasticsearchDestinationS3BackupProps;
    if (!this.props.s3Backup) {
      backupProps = {
        mode: BackupMode.FAILED,
      };
    } else if (!this.props.s3Backup.mode) {
      backupProps = {
        mode: BackupMode.FAILED,
        ...this.props.s3Backup,
      };
    } else {
      backupProps = this.props.s3Backup;
    }

    const { backupConfig, dependables: backupDependables } = createBackupConfig(scope, role, backupProps) ?? {};
    if (!backupConfig) {
      throw new Error('Failed to create s3 backup configuration for Elasticsearch destination.');
    }

    return {
      elasticsearchDestinationConfiguration: {
        bufferingHints: createBufferingHints(this.props.bufferingInterval, this.props.bufferingSize),
        cloudWatchLoggingOptions: loggingOptions,
        domainArn: this.domain.domainArn,
        indexName: this.props.indexName,
        indexRotationPeriod: this.props.indexRotation,
        processingConfiguration: createProcessingConfig(scope, role, this.props.processor),
        retryOptions: this.props.retryInterval
          ? { durationInSeconds: this.props.retryInterval.toSeconds() }
          : undefined,
        roleArn: role.roleArn,
        s3BackupMode: this.getS3BackupMode(),
        s3Configuration: backupConfig,
        typeName: this.props.typeName,
      },
      dependables: [...(loggingDependables ?? []), ...(backupDependables ?? [])],
    };
  }

  private getS3BackupMode(): string | undefined {
    return this.props.s3Backup?.mode === BackupMode.ALL
      ? 'AllDocuments'
      : 'FailedDocumentsOnly';
  }

  private validateIndexName(): void {
    const startsWithUnderscore = /^_.*/;
    const capitalLetters = /[A-Z].*/;
    const commas = /,/;

    if (startsWithUnderscore.exec(this.props.indexName)) {
      throw new Error(`Elasticsearch index name must not begin with an underscore. indexName provided: ${this.props.indexName}`);
    }

    if (capitalLetters.exec(this.props.indexName)) {
      throw new Error(`Elasticsearch index name must be lower-case. indexName provided: ${this.props.indexName}`);
    }

    if (commas.exec(this.props.indexName)) {
      throw new Error(`Elasticsearch index name must not contain commas. indexName provided: ${this.props.indexName}`);
    }
  }
}