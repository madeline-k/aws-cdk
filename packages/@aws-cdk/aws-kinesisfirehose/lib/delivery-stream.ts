import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import { IResource, RemovalPolicy, Resource, Stack } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { IDeliveryStreamDestination } from './delivery-stream-destination';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

/**
 * Represents a Kinesis Data Firehose delivery stream.
 */
export interface IDeliveryStream extends IResource {
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
   * Grant the indicated permissions on this delivery stream to the provided IAM principal.
   */
  grant(grantee: iam.IGrantable, ...actions: string[]): iam.Grant;

  /**
   * Return delivery stream metric based from its metric name
   *
   * @param metricName name of the delivery stream metric
   * @param props properties of the metric
   */
  metric(metricName: string, props?: cloudwatch.MetricOptions): cloudwatch.Metric;
}

/**
 * Represents a Kinesis Firehose Delivery Stream
 */
abstract class DeliveryStreamBase extends Resource implements IDeliveryStream {
  public abstract readonly deliveryStreamArn: string;
  public abstract readonly deliveryStreamName: string;

  /**
   * Grant the indicated permissions on this delivery stream to the given IAM principal (Role/Group/User).
   */
  public grant(grantee: iam.IGrantable, ...actions: string[]) {
    return iam.Grant.addToPrincipal({
      grantee,
      actions,
      resourceArns: [this.deliveryStreamName],
      scope: this,
    });
  }

  /**
   * Return delivery stream metric based from its metric name
   *
   * @param metricName name of the stream metric
   * @param props properties of the metric
   */
  public metric(metricName: string, props?: cloudwatch.MetricOptions) {
    return new cloudwatch.Metric({
      namespace: 'AWS/Firehose',
      metricName,
      dimensions: {
        StreamName: this.deliveryStreamName,
      },
      ...props,
    }).attachTo(this);
  }
}

/**
 * Properties for a Kinesis Firehose Delivery Stream
 */
export interface DeliveryStreamProps {

  /**
   * The S3 bucket where Kinesis Data Firehose backs up incoming data.
   * @default - A new S3 bucket will be created.
   */
  readonly bucket?: s3.IBucket;

  /**
   * The delivery stream type
   * @default - "DirectPut"
   */
  readonly deliveryStreamType?: string;

  /**
   * Enforces a particular delivery stream name.
   * @default <generated>
   */
  readonly deliveryStreamName?: string;

  /**
   * The delivery stream destination.
   */
  readonly destination: IDeliveryStreamDestination;

  /**
   * The IAM role associated with this delivery stream.
   * @default - A new role will be created.
   */
  readonly role?: iam.IRole;
}


/**
 * A Kinesis Firehose Delivery Stream
 */
export class DeliveryStream extends DeliveryStreamBase {

  /**
   * Import an existing Kinesis Firehose Delivery Stream provided an ARN.
   *
   * @param scope The parent creating construct (usually `this`).
   * @param id The construct's name
   * @param deliveryStreamArn Delivery Stream ARN (i.e. arn:aws:firehose:<region>:<account-id>:deliverystream/Foo
   */
  public static fromDeliveryStreamArn(scope: Construct, id: string, deliveryStreamArn: string): IDeliveryStream {
    class Import extends DeliveryStreamBase {
      public readonly deliveryStreamArn = deliveryStreamArn;
      public readonly deliveryStreamName = Stack.of(scope).parseArn(deliveryStreamArn).resourceName!;
    }

    return new Import(scope, id);
  }

  public readonly deliveryStreamArn: string;
  public readonly deliveryStreamName: string;

  private readonly bucket: s3.IBucket;
  private readonly deliveryStream: CfnDeliveryStream;
  private readonly role: iam.IRole;

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    this.role = props.role || new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    this.bucket = props.bucket || new s3.Bucket(this, 'Bucket', {
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.bucket.grantReadWrite(this.role);

    const destinationConfig = props.destination.bind({
      role: this.role,
      bucket: this.bucket,
    });

    this.deliveryStream = new CfnDeliveryStream(this, 'Resource', {
      deliveryStreamType: props.deliveryStreamType ?? 'DirectPut',
      ...destinationConfig.properties,
    });
    this.deliveryStream.node.addDependency(this.role);

    this.deliveryStreamArn = this.getResourceArnAttribute(this.deliveryStream.attrArn, {
      service: 'kinesis',
      resource: 'deliverystream',
      resourceName: this.physicalName,
    });
    this.deliveryStreamName = this.getResourceNameAttribute(this.deliveryStream.ref);
  }
}