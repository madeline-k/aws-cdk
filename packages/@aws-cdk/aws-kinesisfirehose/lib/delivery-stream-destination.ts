import * as elasticsearch from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import { Duration, Size } from '@aws-cdk/core';
import { IDeliveryStream } from './delivery-stream';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

/**
 * Possible compression options Firehose can use to compress data on delivery
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
 * The destination of the data published to a delivery stream
 */
export interface IDeliveryStreamDestination {
  /**
   * TODO Add doc
   */
  bind(scope: IDeliveryStream): DestinationConfig;
}

/**
 * Construction properties for an Elasticsearch destination
 */
export interface ElasticSearchDestinationProps {

  /**
   * The Elasticsearch domain
   */
  readonly domain: elasticsearch.IDomain;

  /**
   * The name of the Elasticsearch domain index
   */
  readonly indexName: string;
}

/**
 * Configuration for delivery stream destinations
 */
export interface DestinationConfig {
  /**
   * Schema-less properties that will be injected directly into `CfnDeliveryStream`.
   */
  readonly properties: object;
}

/**
 * Elasticsearch destination of a delivery stream
 */
export class ElasticSearchDestination implements IDeliveryStreamDestination {

  /**
   * The Elasticsearch domain
   */
  readonly domain: elasticsearch.IDomain;

  /**
   * The name of the Elasticsearch domain index
   */
  readonly indexName: string;

  constructor(props: ElasticSearchDestinationProps) {
    this.domain = props.domain;
    this.indexName = props.indexName;
  }

  /**
   * TODO Add doc
   */
  public bind(scope: IDeliveryStream): DestinationConfig {
    this.domain.grantReadWrite(scope.role);

    // TODO Question: is it a good practice to mutate objects in this method?
    scope.role.addToPolicy(new iam.PolicyStatement({
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

    return {
      properties: {
        elasticsearchDestinationConfiguration: {
          indexName: this.indexName,
          roleArn: scope.role.roleArn,
          s3Configuration: {
            bucketArn: scope.bucket.bucketArn,
            roleArn: scope.role.roleArn,
          },
          domainArn: this.domain.domainArn,
        },
      },
    };
  }
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
export class S3Destination implements IDeliveryStreamDestination {
  constructor(private readonly props: S3DestinationProps) {
  }

  /**
   * TODO Add doc
   */
  bind(stream: IDeliveryStream): DestinationConfig {
    this.props.bucket.grantReadWrite(stream.role);

    return {
      properties: {
        s3DestinationConfiguration: {
          bucketArn: this.props.bucket.bucketArn,
          roleArn: stream.role.roleArn,
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

function encryptionConfigurationProperty(encryptionKey?: kms.IKey): CfnDeliveryStream.EncryptionConfigurationProperty {
  return encryptionKey != null
    ? { kmsEncryptionConfig: { awskmsKeyArn: encryptionKey.keyArn } }
    : { noEncryptionConfig: 'NoEncryption' };
}

function bufferingHintsProperty(bufferingHints?: BufferingHints): CfnDeliveryStream.BufferingHintsProperty | undefined {
  if (bufferingHints != null) {
    return {
      sizeInMBs: bufferingHints?.size.toMebibytes(),
      intervalInSeconds: bufferingHints?.interval.toSeconds(),
    };
  }

  return undefined;
}