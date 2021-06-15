import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as iam from '@aws-cdk/aws-iam';
import * as kinesis from '@aws-cdk/aws-kinesis';
import * as kms from '@aws-cdk/aws-kms';
import * as cdk from '@aws-cdk/core';
import { RegionInfo } from '@aws-cdk/region-info';
import { Construct } from 'constructs';
import { IDestination } from './destination';
import { FirehoseMetrics } from './kinesisfirehose-canned-metrics.generated';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

/**
 * Represents a Kinesis Data Firehose delivery stream.
 */
export interface IDeliveryStream extends cdk.IResource, iam.IGrantable, ec2.IConnectable, cdk.ITaggable {
  /**
   * The ARN of the delivery stream.
   *
   * @attribute
   */
  readonly deliveryStreamArn: string;

  /**
   * The name of the delivery stream
   *
   * @attribute
   */
  readonly deliveryStreamName: string;

  /**
   * Grant the `grantee` identity permissions to perform `actions`.
   */
  grant(grantee: iam.IGrantable, ...actions: string[]): iam.Grant;

  /**
   * Grant the `grantee` identity permissions to write data to this stream.
   */
  grantWrite(grantee: iam.IGrantable): iam.Grant;

  /**
   * Return the given named metric for this delivery stream
   */
  metric(metricName: string, props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Metric for the number of bytes ingested successfully into the delivery stream over the specified time period after throttling.
   */
  metricIncomingBytes(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Metric for the number of records ingested successfully into the delivery stream over the specified time period after throttling.
   */
  metricIncomingRecords(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Metric for the number of bytes delivered to Amazon S3 for backup over the specified time period.
   */
  metricBackupToS3Bytes(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Metric for the age (from getting into Kinesis Data Firehose to now) of the oldest record in Kinesis Data Firehose.
   *
   * Any record older than this age has been delivered to the Amazon S3 bucket for backup.
   */
  metricBackupToS3DataFreshness(props?: cloudwatch.MetricOptions): cloudwatch.Metric;

  /**
   * Metric for the number of records delivered to Amazon S3 for backup over the specified time period.
   */
  metricBackupToS3Records(props?: cloudwatch.MetricOptions): cloudwatch.Metric;
}

/**
 * Base class for new and imported Kinesis Data Firehose delivery streams
 */
export abstract class DeliveryStreamBase extends cdk.Resource implements IDeliveryStream {

  abstract readonly deliveryStreamName: string;

  abstract readonly deliveryStreamArn: string;

  abstract readonly grantPrincipal: iam.IPrincipal;

  public readonly connections: ec2.Connections;

  public readonly tags = new cdk.TagManager(cdk.TagType.STANDARD, 'AWS::KinesisFirehose::DeliveryStream');

  constructor(scope: Construct, id: string) {
    super(scope, id);

    this.connections = setConnections(this);
  }

  public grant(grantee: iam.IGrantable, ...actions: string[]): iam.Grant {
    return iam.Grant.addToPrincipal({
      resourceArns: [this.deliveryStreamArn],
      grantee: grantee,
      actions: actions,
    });
  }

  public grantWrite(grantee: iam.IGrantable): iam.Grant {
    return this.grant(grantee, 'firehose:PutRecord', 'firehose:PutRecordBatch');
  }

  public metric(metricName: string, props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return new cloudwatch.Metric({
      namespace: 'AWS/Firehose',
      metricName: metricName,
      dimensions: {
        DeliveryStreamName: this.deliveryStreamName,
      },
      ...props,
    }).attachTo(this);
  }

  /**
   * By default, this metric will be calculated as an average over a period of 5 minutes.
   */
  public metricIncomingBytes(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return this.cannedMetric(FirehoseMetrics.incomingBytesAverage, props);
  }

  /**
   * By default, this metric will be calculated as an average over a period of 5 minutes.
   */
  public metricIncomingRecords(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return this.cannedMetric(FirehoseMetrics.incomingRecordsAverage, props);
  }

  /**
   * By default, this metric will be calculated as an average over a period of 5 minutes.
   */
  public metricBackupToS3Bytes(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return this.cannedMetric(FirehoseMetrics.backupToS3BytesAverage, props);
  }

  /**
   * By default, this metric will be calculated as an average over a period of 5 minutes.
   */
  public metricBackupToS3DataFreshness(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return this.cannedMetric(FirehoseMetrics.backupToS3DataFreshnessAverage, props);
  }

  /**
   * By default, this metric will be calculated as an average over a period of 5 minutes.
   */
  public metricBackupToS3Records(props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return this.cannedMetric(FirehoseMetrics.backupToS3RecordsAverage, props);
  }

  private cannedMetric(fn: (dims: { DeliveryStreamName: string }) => cloudwatch.MetricProps, props?: cloudwatch.MetricOptions): cloudwatch.Metric {
    return new cloudwatch.Metric({
      ...fn({ DeliveryStreamName: this.deliveryStreamName }),
      ...props,
    }).attachTo(this);
  }
}

/**
 * Options for server-side encryption of a delivery stream
 */
export enum StreamEncryption {
  /**
   * Data in the stream is stored unencrypted.
   */
  UNENCRYPTED,

  /**
   * Data in the stream is stored encrypted by a KMS key managed by the customer.
   */
  CUSTOMER_MANAGED,

  /**
   * Data in the stream is stored encrypted by a KMS key owned by AWS and managed for use in multiple AWS accounts.
   */
  AWS_OWNED
}

/**
 * Properties for a new delivery stream
 */
export interface DeliveryStreamProps {
  /**
   * The destination that this delivery stream will deliver data to.
   *
   * TODO: figure out if multiple destinations are supported (describe stream API return value seems to indicate so) and convert this to a list
   */
  readonly destination: IDestination;

  /**
   * A name for the delivery stream.
   *
   * @default - a name is generated by CloudFormation.
   */
  readonly deliveryStreamName?: string;

  /**
   * The Kinesis data stream to use as a source for this delivery stream.
   *
   * @default - data is written to the delivery stream via a direct put.
   */
  readonly sourceStream?: kinesis.IStream;

  /**
   * The IAM role assumed by Kinesis Firehose to read from sources, invoke processors, and write to destinations
   *
   * @default - a role will be created with default permissions.
   */
  readonly role?: iam.IRole;

  // TODO: move bucket from destination?
  /**
   * The S3 bucket where Kinesis Data Firehose backs up incoming data.
   *
   * @default - A new S3 bucket will be created.
   */
  // readonly bucket?: s3.IBucket;

  /**
   * Indicates the type of customer master key (CMK) to use for server-side encryption, if any.
   *
   * If `encryptionKey` is provided, this will be implicitly set to `CUSTOMER_MANAGED`.
   *
   * @default StreamEncryption.UNENCRYPTED.
   */
  readonly encryption?: StreamEncryption;

  /**
   * Customer managed key to server-side encrypt data in the stream.
   *
   * @default - if `encryption` is set to `CUSTOMER_MANAGED`, a KMS key will be created for you.
   */
  readonly encryptionKey?: kms.IKey;
}

/**
 * A full specification of a delivery stream that can be used to import it fluently into the CDK application.
 */
export interface DeliveryStreamAttributes {
  /**
   * The ARN of the delivery stream.
   *
   * At least one of deliveryStreamArn and deliveryStreamName must be provided.
   *
   * @default - derived from `deliveryStreamName`.
   */
  readonly deliveryStreamArn?: string;

  /**
   * The name of the delivery stream
   *
   * At least one of deliveryStreamName and deliveryStreamArn  must be provided.
   *
   * @default - derived from `deliveryStreamArn`.
   */
  readonly deliveryStreamName?: string;


  /**
   * The IAM role associated with this delivery stream.
   *
   * Assumed by Kinesis Firehose to read from sources, invoke processors, and write to destinations.
   *
   * @default - the imported stream cannot be granted access to other resources as an `iam.IGrantable`.
   */
  readonly role?: iam.IRole;
}

/**
 * Create a Kinesis Data Firehose delivery stream
 *
 * @resource AWS::KinesisFirehose::DeliveryStream
 */
export class DeliveryStream extends DeliveryStreamBase {
  /**
   * Import an existing delivery stream from its name.
   */
  static fromDeliveryStreamName(scope: Construct, id: string, deliveryStreamName: string): IDeliveryStream {
    return this.fromDeliveryStreamAttributes(scope, id, { deliveryStreamName });
  }

  /**
   * Import an existing delivery stream from its ARN.
   */
  static fromDeliveryStreamArn(scope: Construct, id: string, deliveryStreamArn: string): IDeliveryStream {
    return this.fromDeliveryStreamAttributes(scope, id, { deliveryStreamArn });
  }

  /**
   * Import an existing delivery stream from its attributes.
   */
  static fromDeliveryStreamAttributes(scope: Construct, id: string, attrs: DeliveryStreamAttributes): IDeliveryStream {
    if (!attrs.deliveryStreamName && !attrs.deliveryStreamArn) {
      throw new Error('Either deliveryStreamName or deliveryStreamArn must be provided in DeliveryStreamAttributes');
    }
    const deliveryStreamName = attrs.deliveryStreamName ?? cdk.Stack.of(scope).parseArn(attrs.deliveryStreamArn!).resourceName;
    if (!deliveryStreamName) {
      throw new Error(`Could not import delivery stream from malformatted ARN ${attrs.deliveryStreamArn}: could not determine resource name`);
    }
    const deliveryStreamArn = attrs.deliveryStreamArn ?? cdk.Stack.of(scope).formatArn({
      service: 'firehose',
      resource: 'deliverystream',
      resourceName: attrs.deliveryStreamName,
    });
    class Import extends DeliveryStreamBase {
      public readonly deliveryStreamName = deliveryStreamName!;
      public readonly deliveryStreamArn = deliveryStreamArn;
      public readonly grantPrincipal = attrs.role ?? new iam.UnknownPrincipal({ resource: this });
    }
    return new Import(scope, id);
  }

  readonly deliveryStreamName: string;

  readonly deliveryStreamArn: string;

  readonly grantPrincipal: iam.IPrincipal;

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    const role = props.role ?? new iam.Role(this, 'Service Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });
    this.grantPrincipal = role;

    // TODO: move bucket from destination props?
    /*
    const bucket = props.bucket || new s3.Bucket(this, 'Bucket');
    bucket.grantReadWrite(this);
    */

    if ((props.encryption === StreamEncryption.AWS_OWNED || props.encryption === StreamEncryption.UNENCRYPTED) && props.encryptionKey) {
      throw new Error(`Specified stream encryption as ${props.encryption} but provided a customer-managed key`);
    }
    const encryptionKey = props.encryptionKey ?? (props.encryption === StreamEncryption.CUSTOMER_MANAGED ? new kms.Key(this, 'Key') : undefined);
    const encryptionConfig = (encryptionKey || (props.encryption === StreamEncryption.AWS_OWNED)) ? {
      keyArn: encryptionKey?.keyArn,
      keyType: encryptionKey ? 'CUSTOMER_MANAGED_CMK' : 'AWS_OWNED_CMK',
    } : undefined;
    encryptionKey?.grantEncryptDecrypt(role);

    const streamSourceConfig = props.sourceStream ? {
      kinesisStreamArn: props.sourceStream.streamArn,
      roleArn: role.roleArn,
    } : undefined;
    props.sourceStream?.grantRead(role);

    const destinationConfig = props.destination.bind(this, { deliveryStream: this });

    const resource = new CfnDeliveryStream(this, 'Resource', {
      deliveryStreamEncryptionConfigurationInput: encryptionConfig,
      deliveryStreamName: props.deliveryStreamName,
      deliveryStreamType: props.sourceStream ? 'KinesisStreamAsSource' : 'DirectPut',
      kinesisStreamSourceConfiguration: streamSourceConfig,
      ...destinationConfig.properties,
    });
    resource.node.addDependency(this.grantPrincipal);

    this.deliveryStreamArn = this.getResourceArnAttribute(resource.attrArn, {
      service: 'kinesis',
      resource: 'deliverystream',
      resourceName: this.physicalName,
    });
    this.deliveryStreamName = this.getResourceNameAttribute(resource.ref);
  }
}

function setConnections(scope: Construct) {
  const region = cdk.Stack.of(scope).region;
  let cidrBlock = RegionInfo.get(region).firehoseCidrBlock;
  if (!cidrBlock) {
    const mapping: {[region: string]: { FirehoseCidrBlock: string }} = {};
    RegionInfo.regions.forEach((regionInfo) => {
      if (regionInfo.firehoseCidrBlock) {
        mapping[regionInfo.name] = {
          FirehoseCidrBlock: regionInfo.firehoseCidrBlock,
        };
      }
    });
    const cfnMapping = new cdk.CfnMapping(scope, 'Firehose CIDR Mapping', {
      mapping,
    });
    cidrBlock = cdk.Fn.findInMap(cfnMapping.logicalId, region, 'FirehoseCidrBlock');
    // TODO: this fails deployment if the region isn't configured, is that acceptable?
  }

  return new ec2.Connections({
    peer: ec2.Peer.ipv4(cidrBlock),
  });
}
